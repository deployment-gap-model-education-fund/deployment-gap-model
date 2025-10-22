"""Functions to transform LBNL ISO queue tables."""

import logging
from typing import Dict, List

import numpy as np
import pandas as pd

from dbcp.helpers import add_fips_ids
from dbcp.transform.helpers import (
    add_county_fips_with_backup_geocoding,
    deduplicate_same_physical_entities,
    normalize_multicolumns_to_rows,
)
from dbcp.transform.interconnection_queue_helpers import (
    add_actionable_and_nearly_certain_classification,
    clean_resource_type,
    manual_county_state_name_fixes,
    normalize_point_of_interconnection,
    parse_date_columns,
)

logger = logging.getLogger(__name__)


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
    projects = deduplicate_same_physical_entities(
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
    projects.loc[is_active_project] = add_actionable_and_nearly_certain_classification(
        projects.loc[is_active_project], status_col="interconnection_status_lbnl"
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
    new_locs = manual_county_state_name_fixes(lbnl_normalized_dfs["iso_locations"])
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
    new_locs.loc[new_locs.county_id_fips.eq("51515"), "county_id_fips"] = (
        "51019"  # https://www.ddorn.net/data/FIPS_County_Code_Changes.pdf
    )
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
    assert lbnl_normalized_dfs["iso_projects"]["queue_status"].isna().sum() <= 42, (
        "Unexpected number of projects missing queue status."
    )
    lbnl_normalized_dfs["iso_projects"]["queue_status"] = lbnl_normalized_dfs[
        "iso_projects"
    ]["queue_status"].fillna("withdrawn")

    return lbnl_normalized_dfs


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
    # In 2024 the county_state_pairs and fips_codes columns were added.
    # The fips_codes column needs some cleaning and has less coverage than
    # our geocoded county FIPS. For now, drop the fips_codes column,
    # but later we can update this to validate the geocoded county FIPS
    # with the fips_codes column. The county_state_pairs column is not
    # useful to us at this time.
    project_df = lbnl_df.drop(
        columns=county_cols + ["raw_state_name", "county_state_pairs", "fips_codes"]
    )

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


def _prep_for_deduplication(df: pd.DataFrame) -> None:
    df["point_of_interconnection_clean"] = normalize_point_of_interconnection(
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
    ].fillna("")  # copy
    nan_fips.loc[:, "raw_county_name"] = (
        nan_fips.loc[:, "raw_county_name"]
        .str.lower()
        .str.replace("^city of (.+)", lambda x: x.group(1) + " city", regex=True)
    )
    nan_fips = add_fips_ids(
        nan_fips, state_col="raw_state_name", county_col="raw_county_name"
    )

    locs = location_df.copy()
    locs.loc[:, "county_id_fips"].fillna(nan_fips["county_id_fips"], inplace=True)
    return locs


if __name__ == "__main__":
    # debugging entry point
    import dbcp

    lbnl_uri = "gs://dgm-archive/lbnl_iso_queue/queues_2023_clean_data.xlsx"
    lbnl_raw_dfs = dbcp.extract.lbnl_iso_queue.extract(lbnl_uri)
    lbnl_transformed_dfs = dbcp.transform.lbnl_iso_queue.transform(lbnl_raw_dfs)

    assert lbnl_transformed_dfs
