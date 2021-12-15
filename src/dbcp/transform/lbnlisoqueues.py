"""Functions to transform EIP Infrastructure tables."""

import logging
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)


def active_iso_queue_projects(active_projects: pd.DataFrame) -> pd.DataFrame:
    """Transform active iso queue data."""
    # TODO: apply transformations
    return active_projects


def completed_iso_queue_projects(completed_projects: pd.DataFrame) -> pd.DataFrame:
    """Transform completed iso queue data."""
    # TODO: apply transformations
    return completed_projects


def withdrawn_iso_queue_projects(withdrawn_projects: pd.DataFrame) -> pd.DataFrame:
    """Transform withdrawn iso queue data."""
    # TODO: apply transformations
    return withdrawn_projects


def transform(lbnl_raw_dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Transform LBNL ISO Queues dataframes.

    Args:
        lbnl_raw_dfs: Dictionary of the raw extracted data for each table.

    Returns:
        lbnl_transformed_dfs: Dictionary of the transformed tables.
    """
    lbnl_transformed_dfs = {}

    lbnl_transform_functions = {
        "active_iso_queue_projects": active_iso_queue_projects,
        "completed_iso_queue_projects": completed_iso_queue_projects,
        "withdrawn_iso_queue_projects": withdrawn_iso_queue_projects,
    }

    for table_name, transform_func in lbnl_transform_functions.items():
        logger.info(f"LBNL ISO Queues: Transforming {table_name} table.")

        table_df = lbnl_raw_dfs[table_name].copy()
        lbnl_transformed_dfs[table_name] = transform_func(table_df)

    return lbnl_transformed_dfs
