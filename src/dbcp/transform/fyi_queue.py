"""Functions to transform interconnection.FYI interconnection queue tables."""

import logging
from typing import Dict

import pandas as pd
import yaml

from dbcp.constants import FYI_RESOURCE_DICT, OUTPUT_DIR
from dbcp.transform.helpers import (
    add_county_fips_with_backup_geocoding,
    deduplicate_same_physical_entities,
)
from dbcp.transform.interconnection_queue_helpers import (
    add_actionable_and_nearly_certain_classification,
    clean_resource_type,
    manual_county_state_name_fixes,
    normalize_point_of_interconnection,
    parse_date_columns,
)

logger = logging.getLogger(__name__)


def _validate_interconnection_status_fyi(statuses: pd.Series) -> pd.Series:
    """Validate the interconnection_status_fyi values."""
    statuses = statuses.str.strip()
    allowed_statuses = {
        "Cluster Study",
        "Construction",
        "Facility Study",
        "Feasibility Study",
        "IA Executed",
        "IA Pending",
        "In Progress (unknown study)",
        "Not Started",
        "Operational",
        "Suspended",
        "System Impact Study",
        "Withdrawn",
    }

    bad = statuses.loc[~statuses.isin(allowed_statuses) & statuses.notna()]
    assert len(bad) == 0, f"Unknown interconnection status(es):\n{bad.value_counts()}"


def _prep_for_deduplication(df: pd.DataFrame) -> None:
    df["point_of_interconnection_clean"] = normalize_point_of_interconnection(
        df["point_of_interconnection"]
    )
    df["utility_clean"] = df["utility"].fillna(df["power_market"])

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
        df["interconnection_status_fyi"]
        .str.strip()
        .str.lower()
        .map(status_map)
        .fillna(-1)
    ).astype(int)
    return


def _clean_all_fyi_projects(raw_projects: pd.DataFrame) -> pd.DataFrame:
    """Transform active, operational and withdrawn iso queue projects."""
    projects = raw_projects.copy()
    rename_dict = {
        "state": "raw_state_name",
        "county": "raw_county_name",
        "unique_id": "project_id",
        "raw_developer": "developer_raw",
    }
    assert (
        projects.unique_id.is_unique
    ), "unique_id is not unique in the raw interconnection.FYI data!"
    projects = projects.rename(columns=rename_dict)
    # the interconnection_status_fyi column is already a cleaned
    # version of interconnection_status_raw, but validate to see
    # if there are any new or unexpected values
    _validate_interconnection_status_fyi(projects.loc[:, "interconnection_status_fyi"])
    # convert date columns to datetime dtype
    parse_date_columns(projects)
    projects = projects.drop(
        columns=["schedule_next_event_date_raw", "most_recent_study_date_raw"]
    )
    # deduplicate
    pre_dedupe = len(projects)
    projects = deduplicate_same_physical_entities(
        projects,
        key=[
            "point_of_interconnection_clean",  # derived in _prep_for_deduplication
            "capacity_mw",
            "raw_county_name",
            "raw_state_name",
            "utility_clean",  # derived in _prep_for_deduplication
            "canonical_generation_types",
            "queue_status",
        ],
        tiebreak_cols=[  # first priority to last
            "proposed_completion_date",
            "status_rank",  # derived in _prep_for_deduplication
            "queue_date",
        ],
        intermediate_creator=_prep_for_deduplication,
    )
    n_dupes = pre_dedupe - len(projects)
    logger.info(f"Deduplicated {n_dupes} ({n_dupes / pre_dedupe:.2%}) projects.")

    projects.set_index("project_id", inplace=True)
    projects.sort_index(inplace=True)
    # clean up whitespace
    for col in projects.columns:
        if pd.api.types.is_object_dtype(projects.loc[:, col]):
            projects.loc[:, col] = projects.loc[:, col].str.strip()
    # add queue_year
    projects["queue_year"] = projects["queue_date"].dt.year
    projects["queue_status"] = projects["queue_status"].str.lower()
    # add is_actionable and is_nearly_certain classifications to active projects
    projects["is_actionable"] = pd.NA
    projects["is_nearly_certain"] = pd.NA
    is_active_project = projects.queue_status.eq("active")
    projects.loc[is_active_project] = add_actionable_and_nearly_certain_classification(
        projects.loc[is_active_project], status_col="interconnection_status_fyi"
    )
    # assert is_actionable and is_nearly_certain are all null for operational and withdrawn projects
    assert (
        projects.loc[~is_active_project, ["is_actionable", "is_nearly_certain"]]
        .isna()
        .all()
        .all()
    ), "Some operational or withdrawn projects have is_actionable or is_nearly_certain values."

    # Replace ISO-NE values in region with ISONE to match gridstatus
    projects["power_market"] = projects["power_market"].replace({"ISO-NE": "ISONE"})

    return projects


def parse_capacity(row):
    """Parse the capacity_by_generation_type_breakdown column into separate columns."""
    capacity_yaml_str = row["capacity_by_generation_type_breakdown"]
    if capacity_yaml_str == "nan":
        return {"resource": None, "capacity_mw": None}
    try:
        data = yaml.safe_load(capacity_yaml_str)
    except Exception:
        return {"resource": None, "capacity_mw": None}
    # in NYISO battery storage is reported with mwh, we don't actually
    # do anything with this but it could be useful if
    # we want to save total energy storage capacity later
    allowed_keys = {"canonical_gen_type", "mw", "mwh"}
    keys = {key for item in data for key in item.keys()}
    assert (
        len(keys - allowed_keys) == 0
    ), f"New key found in the capacity_by_generation_type_breakdown yaml string: {keys - allowed_keys}. For project_id: {row.name}"
    # remove the mwh keys, we don't do anything with the total battery
    # storage capacity right now
    data = [item for item in data if "mwh" not in item.keys()]
    # if a row is from NYISO we expect it to only have "mwh"
    if row["power_market"] == "NYISO":
        assert len(data) == 0, (
            "A key that isn't 'mwh' was found in capacity_by_generation_type_breakdown of an NYISO project."
            f"Check how this resource should be handled. Row: {row}"
        )
        return {"resource": None, "capacity_mw": None}
    return {
        "resource": [item.get("canonical_gen_type") for item in data],
        "capacity_mw": [
            # item.get("mw") if "mw" in item else item.get("mwh") for item in data
            item.get("mw")
            for item in data
        ],
    }


def _normalize_resource_capacity(fyi_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Pull out capacity and resource values into a separate dataframe.

    Args:
        fyi_df (pd.DataFrame): FYI queue dataframe

    Returns:
        Dict[str, pd.DataFrame]: dict with the projects and multivalues split into two dataframes
    """
    # NYISO, CAISO, and West report capacity broken out by resource type
    # for some projects.
    # Apply parsing to capacity_by_generation_type_breakdown column
    assert set(
        fyi_df[
            ~fyi_df["capacity_by_generation_type_breakdown"].isnull()
        ].power_market.unique()
    ) == set(["CAISO", "West", "NYISO"]), (
        "There's a new power market with non-null values in capacity_by_generation_type_breakdown, check to see how this column interacts with capacity_mw in this power market."
        f"Power markets are: {set(fyi_df[~fyi_df['capacity_by_generation_type_breakdown'].isnull()].power_market.unique())}"
    )
    # We don't actually grab any capacity from NYISO, because it's
    # just the total energy storage capacity that's reported in
    # capacity_by_generation_type_breakdown, but we check in parse_capacity
    # that there is nothing unexpected in this column of NYISO projects.
    parsed = fyi_df.apply(parse_capacity, result_type="expand", axis=1)
    resource_capacity_df = parsed.explode(["resource", "capacity_mw"])
    resource_capacity_df = resource_capacity_df[
        resource_capacity_df["resource"].notnull()
        & resource_capacity_df["capacity_mw"].notnull()
    ].reset_index()

    # occasionally in CAISO the parsed capacity by generation type breakdown
    # will list the same resource twice if there are two generators, in
    # these cases, sum the resources
    n_expected_duped_resources = 10
    assert (
        len(
            resource_capacity_df[
                resource_capacity_df.duplicated(subset=["project_id", "resource"])
            ]
        )
        < n_expected_duped_resources
    ), (
        f"More than {n_expected_duped_resources} projects found with the same resource"
        "listed twice in capacity_by_generation_type_breakdown. Ensure that their capacities should be summed."
        f"They have project IDs: {resource_capacity_df[resource_capacity_df.duplicated(subset=['project_id', 'resource'])].project_id}"
    )
    # There are some projects where summing the duplicated resource's capacity
    # doesn't make sense, i.e. when it seems like a mistake that there are
    # two of the same resource listed or it's a cogen gas plant and only one
    # of the generators provides capacity for the grid.
    # This can be handled by dropping one of the rows
    # of duplicated capacity for each of these projects.
    proj_ids_resource_capacity_not_to_sum = [
        ("ladwp-q57", "Gas", 750),
        ("caiso-955", "Gas", 60),
        ("tucson-electric-power-94", "Solar", 255),
        ("tucson-electric-power-94", "Battery", 255),
    ]
    indices_to_drop = []
    for project_id, resource, cap in proj_ids_resource_capacity_not_to_sum:
        row_to_drop = resource_capacity_df.loc[
            (resource_capacity_df.project_id == project_id)
            & (resource_capacity_df.resource == resource)
            & (resource_capacity_df.capacity_mw == cap)
        ]
        if len(row_to_drop) > 0:
            indices_to_drop.append(row_to_drop.index[0])
    resource_capacity_df = resource_capacity_df.drop(indices_to_drop, axis=0)
    resource_capacity_df = resource_capacity_df.groupby(
        by=["project_id", "resource"], as_index=False
    )["capacity_mw"].sum()
    """
    # NYISO lists only the battery storage total energy storage capacity in
    # capacity_by_generation_type_breakdown. Thus it's necessary
    # to grab the capacity_mw value.
    # For CAISO and West it's fine to use capacity_by_generation_type_breakdown
    # without grabbing additional resources with capacity_mw
    nyiso_multi_cap = fyi_df[
        (fyi_df.power_market == "NYISO")
        & (~fyi_df["capacity_by_generation_type_breakdown"].isnull())
    ].reset_index()
    nyiso_multi_cap["canonical_generation_types"] = (
        nyiso_multi_cap["canonical_generation_types"]
        .str.replace(r"^Battery\s\+\s|\s\+\sBattery", "", regex=True) # noqa: W605
        .str.strip()
    )
    nyiso_multi_cap = nyiso_multi_cap[
        nyiso_multi_cap["canonical_generation_types"] != "Battery"
    ].rename(columns={"canonical_generation_types": "resource"})
    nyiso_multi_cap = nyiso_multi_cap.rename(
        columns={"canonical_generation_types": "resource"}
    )
    # before concatenating the resource - capacity pairs parsed
    # from the two different locations, differentiate which ones are
    # parsed from the capacity_by_generation_type_breakdown column
    # because we are going to prioritize keeping those rows when there
    # are duplicate resources for one project
    resource_capacity_df["parsed_from_capacity_by_generation_type_breakdown"] = True
    nyiso_multi_cap["parsed_from_capacity_by_generation_type_breakdown"] = False
    resource_capacity_df = pd.concat(
        [
            resource_capacity_df,
            nyiso_multi_cap[
                [
                    "project_id",
                    "resource",
                    "capacity_mw",
                    "parsed_from_capacity_by_generation_type_breakdown",
                ]
            ],
        ]
    )
    # In the case of NYISO, where we've pulled capacity values from both capacity_mw and
    # capacity_by_generation_type_breakdown, there may be two capacity
    # values reported for the same resource. After looking at the raw queue
    # values, the capacity in capacity_by_generation_type_breakdown more accurately
    # reflects the complete energy storage capacity available, whereas the
    # capacity_mw value reflects the summer or winter peak capacity. Thus, we
    # choose to keep the capacity_by_generation_type_breakdown in these cases.
    # If there are duplicate resources per project, only keep the one
    # pulled from capacity_by_generation_type_breakdown.
    resource_capacity_df = (
        resource_capacity_df.sort_values(
            by="parsed_from_capacity_by_generation_type_breakdown", ascending=False
        )
        .drop_duplicates(subset=["project_id", "resource"], keep="first")
        .drop(columns=["parsed_from_capacity_by_generation_type_breakdown"])
    )
    """
    # get the rest of the projects which just use capacity_mw and not
    # capacity_by_generation_type_breakdown.
    # Additionally grab all the NYISO projects, which we didn't actually
    # get from capacity_by_generation_type_breakdown
    single_capacity = (
        fyi_df[
            (fyi_df["capacity_by_generation_type_breakdown"].isnull())
            | (fyi_df["power_market"] == "NYISO")
        ]
        .reset_index()
        .rename(columns={"canonical_generation_types": "resource"})
    )
    resource_capacity_df = pd.concat(
        [
            resource_capacity_df,
            single_capacity[["project_id", "resource", "capacity_mw"]],
        ]
    )

    project_df = fyi_df.drop(
        columns=["capacity_mw", "capacity_by_generation_type_breakdown"]
    )

    return {"resource_capacity_df": resource_capacity_df, "project_df": project_df}


def _normalize_location(fyi_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Pull out the county and state columns to a separate dataframe.

    Args:
        fyi_df (pd.DataFrame): FYI queue dataframe

    Returns:
        Dict[str, pd.DataFrame]: dict with the projects and locations split into two dataframes
    """
    location_cols = ["raw_county_name", "raw_state_name", "latitude", "longitude"]
    location_df = fyi_df[location_cols]
    location_df.dropna(
        subset=["raw_state_name", "raw_county_name"], how="all", inplace=True
    )
    # reset project_id index
    location_df = location_df.reset_index()
    project_df = fyi_df.drop(columns=location_cols)
    project_df = project_df.drop(columns=["fips_codes"])
    return {"location_df": location_df, "project_df": project_df}


def normalize_fyi_dfs(fyi_transformed_dfs: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Normalize one-to-many columns and combine the three queues.

    Args:
        fyi_transformed_dfs (Dict[str, pd.DataFrame]): the FYI queue dataframes

    Returns:
        Dict[str, pd.DataFrame]: the combined queues, normalized into projects, locations, and resource_capacity
    """
    resource_capacity_dfs = _normalize_resource_capacity(fyi_transformed_dfs)
    location_dfs = _normalize_location(resource_capacity_dfs["project_df"])
    return {
        "fyi_projects": location_dfs["project_df"],
        "fyi_locations": location_dfs["location_df"],
        "fyi_resource_capacity": resource_capacity_dfs["resource_capacity_df"],
    }


def transform(fyi_raw_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Transform LBNL ISO Queues dataframes.

    Args:
        lbnl_raw_dfs: Dictionary of the raw extracted data for each table.

    Returns:
        lbnl_transformed_dfs: Dictionary of the transformed tables.
    """
    transformed = _clean_all_fyi_projects(
        fyi_raw_dfs["fyi_queue"]
    )  # sets index to project_id

    # Combine and normalize iso queue tables
    fyi_normalized_dfs = normalize_fyi_dfs(transformed)
    # data enrichment
    # Add Fips Codes
    # I write to a new variable because _manual_county_state_name_fixes overwrites
    # raw names with lowercase + manual corrections. I want to preserve raw names in the final
    # output but didn't want to refactor these functions to do it.
    new_locs = manual_county_state_name_fixes(fyi_normalized_dfs["fyi_locations"])
    # add state_id_fips, county_id_fips, geocoded_locality_name, geocoded_locality_type, geocoded_containing_county
    new_locs = add_county_fips_with_backup_geocoding(
        new_locs, state_col="raw_state_name", locality_col="raw_county_name"
    )
    # this doesn't seem to fix any missing FIPS codes in the FYI data
    # new_locs = _fix_independent_city_fips(new_locs)
    new_locs.loc[:, ["raw_state_name", "raw_county_name"]] = (
        fyi_normalized_dfs["fyi_locations"]
        .loc[:, ["raw_state_name", "raw_county_name"]]
        .copy()
    )
    # Fix defunct county FIPS code
    new_locs.loc[
        new_locs.county_id_fips.eq("51515"), "county_id_fips"
    ] = "51019"  # https://www.ddorn.net/data/FIPS_County_Code_Changes.pdf
    fyi_normalized_dfs["fyi_locations"] = new_locs

    # Clean up and categorize resources
    fyi_normalized_dfs["fyi_resource_capacity"] = clean_resource_type(
        fyi_normalized_dfs["fyi_resource_capacity"], FYI_RESOURCE_DICT
    )
    if fyi_normalized_dfs["fyi_resource_capacity"].resource_clean.isna().any():
        raise AssertionError("Missing Resources!")
    fyi_normalized_dfs["fyi_projects"].reset_index(inplace=True)

    # Most projects missing queue_status are from the early 2000s so I'm going to assume
    # they were withrawn.
    assert (
        fyi_normalized_dfs["fyi_projects"]["queue_status"].isna().sum() <= 42
    ), "Unexpected number of projects missing queue status."
    fyi_normalized_dfs["fyi_projects"]["queue_status"] = fyi_normalized_dfs[
        "fyi_projects"
    ]["queue_status"].fillna("Withdrawn")
    return fyi_normalized_dfs


if __name__ == "__main__":
    # debugging entry point
    import dbcp

    fyi_uri = "gs://dgm-archive/interconnection.fyi/interconnection_fyi_dataset_2025-10-01.csv"
    fyi_raw_dfs = dbcp.extract.fyi_queue.extract(fyi_uri)
    fyi_transformed_dfs = dbcp.transform.fyi_queue.transform(fyi_raw_dfs)
    fyi_transformed_dfs["fyi_resource_capacity"].to_parquet(
        OUTPUT_DIR / "private_data_warehouse/fyi_resource_capacity.parquet"
    )

    assert fyi_transformed_dfs
