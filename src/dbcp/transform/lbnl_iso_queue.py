"""Functions to transform LBNL ISO queue tables."""
import logging
from typing import Callable, Dict, List, Sequence

import numpy as np
import pandas as pd

from dbcp.transform.helpers import (
    add_county_fips_with_backup_geocoding,
    normalize_multicolumns_to_rows,
    parse_dates,
)
from pudl.helpers import add_fips_ids as _add_fips_ids

logger = logging.getLogger(__name__)

RESOURCE_DICT = {
    "Battery Storage": {
        "codes": ["Battery", "Batteries", "BAT", "ES"],
        "type": "Renewable",
    },
    "Biofuel": {"codes": ["Biogas"], "type": "Renewable"},
    "Biomass": {"codes": ["Wood", "W", "BLQ WDS", "WDS"], "type": "Renewable"},
    "Coal": {"codes": ["BIT", "C"], "type": "Fossil"},
    "Combustion Turbine": {"codes": ["CT"], "type": "Fossil"},
    "Fuel Cell": {"codes": ["Fuel Cell", "FC"], "type": "Fossil"},
    "Geothermal": {"codes": [], "type": "Renewable"},
    "Hydro": {"codes": ["WAT", "H", "Water"], "type": "Renewable"},
    "Landfill Gas": {"codes": ["LFG", "L", "Landfill", "Waste"], "type": "Fossil"},
    "Municipal Solid Waste": {"codes": ["MSW"], "type": "Fossil"},
    "Natural Gas": {
        "codes": [
            "NG",
            "Methane",
            "Methane Gas",
            "CT-NG",
            "CC",
            "CC-NG",
            "ST-NG",
            "CS-NG",
            "Combined Cycle",
            "Gas",
            "Natural Gas; Other",
            "DFO KER NG",
            "DFO NG",
            "Diesel; Methane",
            "JF KER NG",
            "NG WO",
            "KER NG",
            "Natural Gas; Diesel; Other; Storage",
            "Natural Gas; Oil",
            "Thermal",
        ],
        "type": "Fossil",
    },
    "Nuclear": {"codes": ["NU", "NUC"], "type": "Renewable"},
    "Offshore Wind": {"codes": [], "type": "Renewable"},
    "Oil": {
        "codes": ["DFO", "Diesel", "CT-D", "CC-D", "JF", "KER", "DFO KER", "D"],
        "type": "Fossil",
    },
    "Onshore Wind": {"codes": ["Wind", "WND", "Wind Turbine"], "type": "Renewable"},
    "Other": {"codes": [], "type": "Unknown Resource"},
    "Unknown": {
        "codes": ["Wo", "F", "Hybrid", "M", "Byproduct", "Conventional"],
        "type": "Unknown Resource",
    },
    "Other Storage": {
        "codes": ["Flywheel", "Storage", "CAES", "Gravity Rail", "Hydrogen"],
        "type": "Renewable",
    },
    "Pumped Storage": {
        "codes": ["Pump Storage", "Pumped-Storage hydro", "PS"],
        "type": "Renewable",
    },
    "Solar": {"codes": ["SUN", "S"], "type": "Renewable"},
    "Steam": {"codes": ["ST", "Steam Turbine"], "type": "Fossil"},
    "Waste Heat": {
        "codes": [
            "Waste Heat Recovery",
            "Heat Recovery",
            "Co-Gen",
        ],
        "type": "Fossil",
    },
}


def _harmonize_interconnection_status_lbnl(statuses: pd.Series) -> pd.Series:
    """Harmonize the interconnection_status_lbnl values."""
    statuses = statuses.str.strip()
    mapping = {
        "Feasability Study": "Feasibility Study",
        "Feasibility": "Feasibility Study",
        "Facilities Study": "Facility Study",
        "IA in Progress": "In Progress (unknown study)",
        "Unknown": "In Progress (unknown study)",
        "Withdrawn, Feasibility Study": "Withdrawn",
        "operational": "Operational",
        "withdrawn": "Withdrawn",
        "IA Draft": "In Progress (unknown study)",
        "Facility study": "Facility Study",
        "active": "In Progress (unknown study)",
        "suspended": "Suspended",
    }
    allowed_statuses = {
        "Cluster Study",
        "Combined",
        "Construction",
        "Facility Study",
        "Feasibility Study",
        "IA Executed",
        "IA Pending",
        "In Progress (unknown study)",
        "Not Started",
        "Operational",
        "Phase 4 Study",
        "Suspended",
        "System Impact Study",
        "Withdrawn",
    }
    out = statuses.replace(mapping)
    bad = out.loc[~out.isin(allowed_statuses) & out.notna()]
    assert len(bad) == 0, f"Unknown interconnection status(es):\n{bad.value_counts()}"
    return out


def _clean_all_iso_projects(raw_projects: pd.DataFrame) -> pd.DataFrame:
    """Transform active, operational and withdrawn iso queue projects."""
    projects = raw_projects.copy()
    rename_dict = {
        "state": "raw_state_name",
        "county": "raw_county_name",
    }
    # manual fix for a project that doesn't have queue date, or status
    # drop row where queue_id is 326 and entity is IP
    projects = projects.drop(
        projects[projects["queue_id"].eq("326") & (projects["entity"].eq("IP"))].index
    )

    projects["project_id"] = np.arange(len(projects), dtype=np.int32)
    projects = projects.rename(columns=rename_dict)  # copy
    projects.loc[
        :, "interconnection_status_lbnl"
    ] = _harmonize_interconnection_status_lbnl(
        projects.loc[:, "interconnection_status_lbnl"]
    )
    parse_date_columns(projects)
    # rename date_withdrawn to withdrawn_date and date_operational to actual_completion_date
    projects.rename(
        columns={
            "date_withdrawn_raw": "withdrawn_date_raw",
            "date_operational_raw": "actual_completion_date_raw",
            "date_withdrawn": "withdrawn_date",
            "date_operational": "actual_completion_date",
        },
        inplace=True,
    )
    # deduplicate
    pre_dedupe = len(projects)
    projects = deduplicate_active_projects(
        projects,
        key=[
            "point_of_interconnection_clean",  # derived in _prep_for_deduplication
            "capacity_mw_resource_1",
            "county_1",
            "raw_state_name",
            "utility_clean",  # derived in _prep_for_deduplication
            "resource_type_1",
            "queue_status",
        ],
        tiebreak_cols=[  # first priority to last
            "date_proposed",
            "status_rank",  # derived in _prep_for_deduplication
            "queue_date",
        ],
        intermediate_creator=_prep_for_deduplication,
    )
    n_dupes = pre_dedupe - len(projects)
    logger.info(f"Deduplicated {n_dupes} ({n_dupes/pre_dedupe:.2%}) projects.")

    projects.set_index("project_id", inplace=True)
    projects.sort_index(inplace=True)
    # clean up whitespace
    for col in projects.columns:
        if pd.api.types.is_object_dtype(projects.loc[:, col]):
            projects.loc[:, col] = projects.loc[:, col].str.strip()

    # add is_actionable and is_nearly_certain classifications to active projects
    projects["is_actionable"] = pd.NA
    projects["is_nearly_certain"] = pd.NA
    is_active_project = projects.queue_status.eq("active")
    projects.loc[is_active_project] = _add_actionable_and_nearly_certain_classification(
        projects.loc[is_active_project]
    )
    # assert is_actionable and is_nearly_certain are all null for operational and withdrawn projects
    assert (
        projects.loc[~is_active_project, ["is_actionable", "is_nearly_certain"]]
        .isna()
        .all()
        .all()
    ), "Some operational or withdrawn projects have is_actionable or is_nearly_certain values."

    # S-C utilities don't list the state which prevents them from being geocoded
    projects.loc[
        projects.entity.eq("S-C") | projects.entity.eq("SC"), "raw_state_name"
    ] = "SC"

    # Replace ISO-NE values in region with ISONE to match gridstatus
    projects["region"] = projects["region"].replace({"ISO-NE": "ISONE"})
    return projects


def transform(lbnl_raw_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Transform LBNL ISO Queues dataframes.

    Args:
        lbnl_raw_dfs: Dictionary of the raw extracted data for each table.

    Returns:
        lbnl_transformed_dfs: Dictionary of the transformed tables.
    """
    transformed = _clean_all_iso_projects(
        lbnl_raw_dfs["lbnl_iso_queue"]
    )  # sets index to project_id

    # Combine and normalize iso queue tables
    lbnl_normalized_dfs = normalize_lbnl_dfs(transformed)

    # data enrichment
    # Add Fips Codes
    # I write to a new variable because _manual_county_state_name_fixes overwrites
    # raw names with lowercase + manual corrections. I want to preserve raw names in the final
    # output but didn't want to refactor these functions to do it.
    new_locs = _manual_county_state_name_fixes(lbnl_normalized_dfs["iso_locations"])
    new_locs = add_county_fips_with_backup_geocoding(
        new_locs, state_col="raw_state_name", locality_col="raw_county_name"
    )
    new_locs = _fix_independent_city_fips(new_locs)
    new_locs.loc[:, ["raw_state_name", "raw_county_name"]] = (
        lbnl_normalized_dfs["iso_locations"]
        .loc[:, ["raw_state_name", "raw_county_name"]]
        .copy()
    )
    # Fix defunct county FIPS code
    new_locs.loc[
        new_locs.county_id_fips.eq("51515"), "county_id_fips"
    ] = "51019"  # https://www.ddorn.net/data/FIPS_County_Code_Changes.pdf
    lbnl_normalized_dfs["iso_locations"] = new_locs

    # Clean up and categorize resources
    lbnl_normalized_dfs["iso_resource_capacity"] = clean_resource_type(
        lbnl_normalized_dfs["iso_resource_capacity"]
    )
    if lbnl_normalized_dfs["iso_resource_capacity"].resource_clean.isna().any():
        raise AssertionError("Missing Resources!")
    lbnl_normalized_dfs["iso_projects"].reset_index(inplace=True)

    # Most projects missing queue_status are from the early 2000s so I'm going to assume
    # they were withrawn.
    assert (
        lbnl_normalized_dfs["iso_projects"]["queue_status"].isna().sum() <= 42
    ), "Unexpected number of projects missing queue status."
    lbnl_normalized_dfs["iso_projects"]["queue_status"] = lbnl_normalized_dfs[
        "iso_projects"
    ]["queue_status"].fillna("withdrawn")

    return lbnl_normalized_dfs


def parse_date_columns(queue: pd.DataFrame) -> None:
    """Identify date columns and parse them to pd.Timestamp.

    Original (unparsed) date columns are preserved but with the suffix '_raw'.

    Args:
        queue (pd.DataFrame): an LBNL ISO queue dataframe
    """
    date_cols = [
        col
        for col in queue.columns
        if (
            (col.startswith("date_") or col.endswith("_date"))
            # datetime columns don't need parsing
            and not pd.api.types.is_datetime64_any_dtype(queue.loc[:, col])
        )
    ]

    # add _raw suffix
    rename_dict: dict[str, str] = dict(
        zip(date_cols, [col + "_raw" for col in date_cols])
    )
    queue.rename(columns=rename_dict, inplace=True)

    for date_col, raw_col in rename_dict.items():
        new_dates = parse_dates(queue.loc[:, raw_col])
        # set obviously bad values to null
        # This is designed to catch NaN values improperly encoded by Excel to 1899 or 1900
        bad = new_dates.dt.year.isin({1899, 1900})
        new_dates.loc[bad] = pd.NaT
        queue.loc[:, date_col] = new_dates
    return


def _normalize_resource_capacity(lbnl_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Pull out the awkward one-to-many columns (type_1, capacity_1, type_2, capacity_2) to a separate dataframe.

    Args:
        lbnl_df (pd.DataFrame): LBNL ISO queue dataframe

    Returns:
        Dict[str, pd.DataFrame]: dict with the projects and multivalues split into two dataframes
    """
    n_multicolumns = 3
    attr_columns = {
        "resource": ["resource_type_" + str(n) for n in range(1, n_multicolumns + 1)],
        "capacity_mw": [
            "capacity_mw_resource_" + str(n) for n in range(1, n_multicolumns + 1)
        ],
    }
    resource_capacity_df = normalize_multicolumns_to_rows(
        lbnl_df,
        attribute_columns_dict=attr_columns,
        preserve_original_names=False,
        dropna=True,
    )
    combined_cols: List[str] = sum(attr_columns.values(), start=[])
    project_df = lbnl_df.drop(columns=combined_cols)

    return {"resource_capacity_df": resource_capacity_df, "project_df": project_df}


def _normalize_location(lbnl_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Pull out the awkward one-to-many columns (county_1, county_2, etc) to a separate dataframe.

    Args:
        lbnl_df (pd.DataFrame): LBNL ISO queue dataframe

    Returns:
        Dict[str, pd.DataFrame]: dict with the projects and locations split into two dataframes
    """
    county_cols = ["county_" + str(n) for n in range(1, 4)]
    location_df = normalize_multicolumns_to_rows(
        lbnl_df,
        attribute_columns_dict={"raw_county_name": county_cols},
        preserve_original_names=False,
        dropna=True,
    )
    location_df = location_df.merge(
        lbnl_df.loc[:, "raw_state_name"], on="project_id", validate="m:1"
    )

    project_df = lbnl_df.drop(columns=county_cols + ["raw_state_name"])

    location_df.dropna(
        subset=["raw_state_name", "raw_county_name"], how="all", inplace=True
    )
    return {"location_df": location_df, "project_df": project_df}


def normalize_lbnl_dfs(lbnl_transformed_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Normalize one-to-many columns and combine the three queues.

    Args:
        lbnl_transformed_dfs (Dict[str, pd.DataFrame]): the LBNL ISO queue dataframes

    Returns:
        Dict[str, pd.DataFrame]: the combined queues, normalized into projects, locations, and resource_capacity
    """
    resource_capacity_dfs = _normalize_resource_capacity(lbnl_transformed_df)
    location_dfs = _normalize_location(resource_capacity_dfs["project_df"])
    return {
        "iso_projects": location_dfs["project_df"],
        "iso_locations": location_dfs["location_df"],
        "iso_resource_capacity": resource_capacity_dfs["resource_capacity_df"],
    }


def clean_resource_type(resource_df: pd.DataFrame) -> pd.DataFrame:
    """Standardize resource types used throughout iso queue tables.

    Args:
        resource_df (pd.DataFrame): normalized lbnl ISO queue resource df.

    Returns:
        pd.DataFrame: A copy of the resource df with a new columns for cleaned resource
            types.

    """
    resource_df = resource_df.copy()
    # Modify RESOURCE DICT for mapping
    long_dict = {}
    for clean_name, code_type_dict in RESOURCE_DICT.items():
        long_dict[clean_name] = clean_name
        for code in code_type_dict["codes"]:
            long_dict[code] = clean_name
    # Map clean resource values into new column
    resource_df["resource_clean"] = resource_df["resource"].fillna("Unknown")
    resource_df["resource_clean"] = resource_df["resource_clean"].map(long_dict)
    unmapped = resource_df["resource_clean"].isna()
    if unmapped.sum() != 0:
        debug = resource_df.loc[unmapped, "resource"].value_counts()
        raise AssertionError(f"Unmapped resource types: {debug}")
    return resource_df


def _normalize_point_of_interconnection(ser: pd.Series) -> pd.Series:
    """String normalization for point_of_interconnection.

    Essentially a poor man's bag-of-words model.
    """
    out = (
        ser.astype("string")
        .str.lower()
        .str.replace("-", " ")
        .str.replace(r"(?:sub)station|kv| at |tbd", "", regex=True)
        .fillna("")
    )
    out = pd.Series(  # make permutation invariant by sorting
        [" ".join(sorted(x)) for x in out.str.split()],
        index=out.index,
        dtype="string",
    ).str.strip()
    out.replace("", pd.NA, inplace=True)
    return out


def _prep_for_deduplication(df: pd.DataFrame) -> None:
    df["point_of_interconnection_clean"] = _normalize_point_of_interconnection(
        df["point_of_interconnection"]
    )
    df["utility_clean"] = df["utility"].fillna(df["region"])

    status_order = [  # from most to least advanced; will be assigned values N to 0
        # Put withdrawn and suspended near the top (assume they are final statuses)
        "operational",
        "construction",
        "withdrawn",
        "suspended",
        "ia executed",
        "ia pending",
        "facility study",
        "system impact study",
        "phase 4 study",
        "feasibility study",
        "cluster study",
        "in progress (unknown study)",
        "combined",
        "not started",
    ]
    # assign numerical values for sorting. Largest value is prioritized.
    status_map = dict(zip(reversed(status_order), range(len(status_order))))
    df["status_rank"] = (
        df["interconnection_status_lbnl"]
        .str.strip()
        .str.lower()
        .map(status_map)
        .fillna(-1)
    ).astype(int)
    return


def deduplicate_active_projects(
    df: pd.DataFrame,
    key: Sequence[str],
    tiebreak_cols: Sequence[str],
    intermediate_creator: Callable[[pd.DataFrame], None],
) -> pd.DataFrame:
    """First draft deduplication of ISO queues.

    The intention here is to identify rows that likely describe the same physical
    project, but are duplicated due to different proposed start dates or IA statuses.
    My assumption is that those kind of duplicates exist to cover contingency or by
    mistake and that only one project can actually be built.

    Args:
        df (pd.DataFrame): a queue dataframe

    Returns:
        pd.DataFrame: queue dataframe with duplicates removed
    """
    df = df.copy()
    original_cols = df.columns
    # create whatever derived columns are needed
    intermediate_creator(df)
    intermediate_cols = df.columns.difference(original_cols)

    tiebreak_cols = list(tiebreak_cols)
    key = list(key)
    # Where there are duplicates, keep the row with the largest values in tiebreak_cols
    # (usually date_proposed, queue_date, and interconnection_status).
    # note that NaT is always sorted to the end, so nth(0) will always choose it last.
    dedupe = (
        df.sort_values(key + tiebreak_cols, ascending=False)
        .groupby(key, as_index=False, dropna=False)
        .nth(0)
    )

    # remove whatever derived columns were created
    dedupe.drop(columns=intermediate_cols, inplace=True)
    return dedupe


def _fix_independent_city_fips(location_df: pd.DataFrame) -> pd.DataFrame:
    """Fix about 50 independent cities with wrong name order.

    Args:
        location_df (pd.DataFrame): normalized ISO locations

    Raises:
        ValueError: if add_county_fips_with_backup_geocoding has not been applied first

    Returns:
        pd.DataFrame: copy of location_df with fewer nan fips codes
    """
    if "county_id_fips" not in location_df.columns:
        raise ValueError("Use add_county_fips_with_backup_geocoding() first.")
    nan_fips = location_df.loc[
        location_df["county_id_fips"].isna(), ["raw_state_name", "raw_county_name"]
    ].fillna(
        ""
    )  # copy
    nan_fips.loc[:, "raw_county_name"] = (
        nan_fips.loc[:, "raw_county_name"]
        .str.lower()
        .str.replace("^city of (.+)", lambda x: x.group(1) + " city", regex=True)
    )
    nan_fips = _add_fips_ids(
        nan_fips, state_col="raw_state_name", county_col="raw_county_name"
    )

    locs = location_df.copy()
    locs.loc[:, "county_id_fips"].fillna(nan_fips["county_id_fips"], inplace=True)
    return locs


def _manual_county_state_name_fixes(location_df: pd.DataFrame) -> pd.DataFrame:
    """Fix around 20 incorrect county, state names.

    Args:
        location_df (pd.DataFrame): normalized ISO locations
    Returns:
        pd.DataFrame: copy of location_df with more correct county, state pairs

    """
    # TODO: of the 77 null county_id_fips, about half of them have missing state
    # values that could probably be inferred by appending "county" to the
    # raw_county_name and extracting the geocoded state, if unique.
    manual_county_state_name_fixes = [
        ["skamania", "or", "skamania", "wa"],
        ["coos & curry", "or", "coos", "or"],
        ["coos & curry", "", "coos", "or"],
        ["lake", "or", "lake county", "or"],
        ["franklin-clinton", "ny", "franklin", "ny"],
        ["san juan", "az", "san juan", "nm"],
        ["hidalgo", "co", "hidalgo", "nm"],
        ["coconino", "co", "coconino", "az"],
        ["antelope & wheeler", "ne", "antelope", "ne"],
        ["linden", "ny", "union", "nj"],
        ["church", "nv", "churchill", "nv"],
        ["churchill/pershing", "ca", "churchill", "nv"],
        ["shasta/trinity", "ca", "shasta", "ca"],
        ["san benito", "nv", "san benito", "ca"],
        ["frqanklin", "me", "franklin", "me"],
        ["logan,menard", "il", "logan", "il"],
        ["clarke", "in", "clark", "il"],
        ["lincoln", "co", "lincoln county", "co"],
        ["new york-nj", "ny", "new york", "ny"],
        ["peneobscot/washington", "me", "penobscot", "me"],
        ["delaware (ok)", "ok", "delaware", "ok"],
        # workaround for bug in addfips library.
        # See https://github.com/fitnr/addfips/issues/8
        ["dryer", "tn", "dyer", "tn"],
        ["churchill", "ca", "churchill", "nv"],
        ["pershing", "ca", "pershing", "nv"],
    ]
    manual_county_state_name_fixes = pd.DataFrame(
        manual_county_state_name_fixes,
        columns=["raw_county_name", "raw_state_name", "clean_county", "clean_state"],
    )

    locs = location_df.copy()
    locs.loc[:, "raw_county_name"] = locs.loc[:, "raw_county_name"].str.lower()
    locs.loc[:, "raw_state_name"] = locs.loc[:, "raw_state_name"].str.lower()
    locs = locs.merge(
        manual_county_state_name_fixes,
        how="left",
        on=["raw_county_name", "raw_state_name"],
    )
    locs.loc[:, "raw_county_name"] = locs.loc[:, "clean_county"].fillna(
        locs.loc[:, "raw_county_name"]
    )
    locs.loc[:, "raw_state_name"] = locs.loc[:, "clean_state"].fillna(
        locs.loc[:, "raw_state_name"]
    )
    locs = locs.drop(["clean_county", "clean_state"], axis=1)
    return locs


def _add_actionable_and_nearly_certain_classification(
    queue: pd.DataFrame,
) -> pd.DataFrame:
    """Add columns is_actionable and is_nearly_certain that classify each project.

    This model was created by a consultant in Excel and translated to python.
    """
    if (
        queue["interconnection_status_lbnl"]
        .isin(
            {
                "Facilities Study",
                "Feasability Study",
            }
        )
        .any()
    ):
        raise ValueError("This function only applies to harmonized IA statuses.")
    # the following sets were manually defined by a consultant
    actionable_ia_statuses = {
        "Facility Study",
        "System Impact Study",
        "Phase 4 Study",
        "IA Pending",
    }
    nearly_certain_ia_statuses = {
        "Construction",
        "IA Executed",
        "Operational",
    }
    queue["is_actionable"] = (
        queue["interconnection_status_lbnl"].isin(actionable_ia_statuses).fillna(False)
    )
    queue["is_nearly_certain"] = (
        queue["interconnection_status_lbnl"]
        .isin(nearly_certain_ia_statuses)
        .fillna(False)
    )

    return queue


if __name__ == "__main__":
    # debugging entry point
    import dbcp

    lbnl_uri = "gs://dgm-archive/lbnl_iso_queue/queues_2023_clean_data.xlsx"
    lbnl_raw_dfs = dbcp.extract.lbnl_iso_queue.extract(lbnl_uri)
    lbnl_transformed_dfs = dbcp.transform.lbnl_iso_queue.transform(lbnl_raw_dfs)

    assert lbnl_transformed_dfs
