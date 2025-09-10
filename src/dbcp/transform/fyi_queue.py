"""Functions to transform interconnection.FYI interconnection queue tables."""

import logging
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)


def _clean_all_iso_projects(raw_projects: pd.DataFrame) -> pd.DataFrame:
    """Transform active, operational and withdrawn iso queue projects."""
    projects = raw_projects.copy()
    rename_dict = {
        "state": "raw_state_name",
        "county": "raw_county_name",
        "unique_id": "project_id",
    }
    projects = projects.rename(columns=rename_dict)
    """

    projects.loc[:, "interconnection_status_lbnl"] = (
        _harmonize_interconnection_status_lbnl(
            projects.loc[:, "interconnection_status_lbnl"]
        )
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
    logger.info(f"Deduplicated {n_dupes} ({n_dupes / pre_dedupe:.2%}) projects.")

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
    ), (
        "Some operational or withdrawn projects have is_actionable or is_nearly_certain values."
    )

    # S-C utilities don't list the state which prevents them from being geocoded
    projects.loc[
        projects.entity.eq("S-C") | projects.entity.eq("SC"), "raw_state_name"
    ] = "SC"

    # Replace ISO-NE values in region with ISONE to match gridstatus
    projects["region"] = projects["region"].replace({"ISO-NE": "ISONE"})
    """
    return projects


def transform(fyi_raw_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Transform LBNL ISO Queues dataframes.

    Args:
        lbnl_raw_dfs: Dictionary of the raw extracted data for each table.

    Returns:
        lbnl_transformed_dfs: Dictionary of the transformed tables.
    """
    transformed = _clean_all_iso_projects(
        fyi_raw_dfs["fyi_queue"]
    )  # sets index to project_id
    """
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
    """
    return transformed


if __name__ == "__main__":
    # debugging entry point
    import dbcp

    fyi_uri = (
        "gs://dgm-archive/inconnection.fyi/interconnection_fyi_dataset_2025-09-01.csv"
    )
    fyi_raw_dfs = dbcp.extract.fyi_queue.extract(fyi_uri)
    fyi_transformed_dfs = dbcp.transform.fyi_queue.transform(fyi_raw_dfs)

    assert fyi_transformed_dfs
