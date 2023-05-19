"""Functions to transform LBNL ISO queue tables."""

from typing import Dict, List

import numpy as np
import pandas as pd

from dbcp.transform.helpers import (
    add_county_fips_with_backup_geocoding,
    normalize_multicolumns_to_rows,
    parse_dates,
)
from pudl.helpers import add_fips_ids as _add_fips_ids

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
    "Landfill Gas": {"codes": ["LFG", "L"], "type": "Fossil"},
    "Municipal Solid Waste": {"codes": ["MSW"], "type": "Fossil"},
    "Natural Gas": {
        "codes": [
            "NG",
            "Methane",
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
    "Unknown": {"codes": ["Wo", "F", "Hybrid", "M"], "type": "Unknown Resource"},
    "Other Storage": {
        "codes": ["Flywheel", "Storage", "CAES", "Gravity Rail", "Hydrogen"],
        "type": "Renewable",
    },
    "Pumped Storage": {
        "codes": ["Pump Storage", "Pumped-Storage hydro", "PS"],
        "type": "Renewable",
    },
    "Solar": {"codes": ["SUN", "S"], "type": "Renewable"},
    "Steam": {"codes": ["ST"], "type": "Fossil"},
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
    mapping = {
        "Feasability Study": "Feasibility Study",
        "Facilities Study": "Facility Study",
        "IA in Progress": "In Progress (unknown study)",
    }
    return statuses.replace(mapping)


def active_iso_queue_projects(active_projects: pd.DataFrame) -> pd.DataFrame:
    """Transform active iso queue data."""
    rename_dict = {
        "state": "raw_state_name",
        "county": "raw_county_name",
    }
    active_projects["project_id"] = np.arange(len(active_projects), dtype=np.int32)
    active_projects = active_projects.rename(columns=rename_dict)  # copy
    active_projects.loc[
        :, "interconnection_status_lbnl"
    ] = _harmonize_interconnection_status_lbnl(
        active_projects.loc[:, "interconnection_status_lbnl"]
    )
    # drop irrelevant columns (structurally all nan due to 'active' filter)
    active_projects.drop(columns=["date_withdrawn", "date_operational"], inplace=True)
    active_projects = remove_duplicates(active_projects)  # sets index to project_id
    parse_date_columns(active_projects)
    # manual fix for duplicate resource type in raw data
    bad_proj_id = 1606
    assert (
        active_projects.loc[bad_proj_id, "project_name"] == "Coleto Creek ESS Addition"
    ), "Manual correction is misidentified."
    active_projects.loc[
        bad_proj_id, "resource_type_1"
    ] = "Coal"  # raw data has two instances of "battery storage"
    return active_projects


def transform(lbnl_raw_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Transform LBNL ISO Queues dataframes.

    Args:
        lbnl_raw_dfs: Dictionary of the raw extracted data for each table.

    Returns:
        lbnl_transformed_dfs: Dictionary of the transformed tables.
    """
    active = lbnl_raw_dfs["lbnl_iso_queue"].query("queue_status == 'active'").copy()
    transformed = active_iso_queue_projects(active)  # sets index to project_id
    for col in transformed.columns:
        if pd.api.types.is_object_dtype(transformed.loc[:, col]):
            transformed.loc[:, col] = transformed.loc[:, col].str.strip()

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
    lbnl_normalized_dfs["iso_locations"] = new_locs

    # Clean up and categorize resources
    lbnl_normalized_dfs["iso_resource_capacity"] = clean_resource_type(
        lbnl_normalized_dfs["iso_resource_capacity"]
    )
    if lbnl_normalized_dfs["iso_resource_capacity"].resource_clean.isna().any():
        raise AssertionError("Missing Resources!")
    lbnl_normalized_dfs["iso_projects"].reset_index(inplace=True)

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
    rename_dict = dict(zip(date_cols, [col + "_raw" for col in date_cols]))
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
    if "capacity_mw_resource_3" in lbnl_df.columns:  # only active projects
        n_multicolumns = 3
    else:
        n_multicolumns = 2
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
    if "county_3" in lbnl_df.columns:  # only active projects are multivalued
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
    else:
        location_df = lbnl_df.loc[
            :, ["raw_state_name", "raw_county_name"]
        ].reset_index()
        project_df = lbnl_df.drop(columns=["raw_state_name", "raw_county_name"])

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


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """First draft deduplication of ISO queues.

    Args:
        df (pd.DataFrame): a queue dataframe

    Returns:
        pd.DataFrame: queue dataframe with duplicates removed
    """
    df = df.copy()
    # do some string cleaning on point_of_interconnection
    # for now "tbd" is mapped to "nan"
    df["point_of_interconnection_clean"] = (
        df["point_of_interconnection"]
        .astype(str)
        .str.lower()
        .str.replace("substation", "")
        .str.replace("kv", "")
        .str.replace("-", " ")
        .str.replace("station", "")
        .str.replace(",", "")
        .str.replace("at", "")
        .str.replace("tbd", "nan")
    )

    df["point_of_interconnection_clean"] = [
        " ".join(sorted(x)) for x in df["point_of_interconnection_clean"].str.split()
    ]
    df["point_of_interconnection_clean"] = df[
        "point_of_interconnection_clean"
    ].str.strip()

    key = [
        "point_of_interconnection_clean",
        "capacity_mw_resource_1",
        "county_1",
        "raw_state_name",
        "region",
        "resource_type_1",
    ]
    df["len_resource_type"] = df.resource_type_lbnl.str.len()
    df.reset_index(drop=True, inplace=True)
    dups = df.copy()
    dups = dups.groupby(key, as_index=False, dropna=False).len_resource_type.max()
    df = dups.merge(df, on=(key + ["len_resource_type"]))
    # merge added duplicates with same len_resource_type, drop these
    df = df[~(df.duplicated(key, keep="first"))]

    # some final cleanup
    df = (
        df.drop(["len_resource_type", "point_of_interconnection_clean"], axis=1)
        .set_index("project_id")
        .sort_index()
    )
    return df


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
        ["bedford", "va", "bedford county", "va"],
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
    # one cross-state project breaks the schema, so remove the second location for now.
    is_cross_state = locs["raw_county_name"].eq("benton (ar)") & locs.loc[
        :, "raw_state_name"
    ].eq("ok")
    assert is_cross_state.sum() == 1, "Expected one match for cross-state project."
    locs = locs.loc[~is_cross_state, :]
    locs = locs.drop(["clean_county", "clean_state"], axis=1)
    return locs
