"""Functions to transform EIP Infrastructure tables."""

import logging
from typing import Dict, Any

import pandas as pd

from dbcp.transform.helpers import EXCEL_EPOCH_ORIGIN, parse_dates, normalize_multicolumns_to_rows

logger = logging.getLogger(__name__)


def active_iso_queue_projects(active_projects: pd.DataFrame) -> pd.DataFrame:
    """Transform active iso queue data."""
    parse_date_columns(active_projects)
    replace_value_with_count_validation(active_projects,
                                        col='state',
                                        val_to_replace='NN',
                                        replacement='CA',
                                        expected_count=2,
                                        )
    return active_projects


def completed_iso_queue_projects(completed_projects: pd.DataFrame) -> pd.DataFrame:
    """Transform completed iso queue data."""
    parse_date_columns(completed_projects)
    return completed_projects


def withdrawn_iso_queue_projects(withdrawn_projects: pd.DataFrame) -> pd.DataFrame:
    """Transform withdrawn iso queue data."""
    parse_date_columns(withdrawn_projects)
    replace_value_with_count_validation(withdrawn_projects,
                                        col='state',
                                        val_to_replace='NN',
                                        replacement='CA',
                                        expected_count=5,
                                        )
    return withdrawn_projects


def transform(lbnl_raw_dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Transform LBNL ISO Queues dataframes.

    Args:
        lbnl_raw_dfs: Dictionary of the raw extracted data for each table.

    Returns:
        lbnl_transformed_dfs: Dictionary of the transformed tables.
    """

    lbnl_transformed_dfs = {name: df.copy()
                            for name, df in lbnl_raw_dfs.items()}
    set_global_project_ids(lbnl_transformed_dfs)

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


def set_global_project_ids(lbnl_dfs: Dict[str, pd.DataFrame]) -> None:
    previous_idx_max = 0
    for df in lbnl_dfs.values():
        new_idx = pd.RangeIndex(previous_idx_max, len(
            df) + previous_idx_max, name='project_id')
        df.set_index(new_idx, inplace=True)
        previous_idx_max = new_idx.max()
    return


def parse_date_columns(queue: pd.DataFrame) -> None:
    date_cols = [col for col in queue.columns if (
        (col.startswith('date_') or col.endswith('_date'))
        # datetime columns don't need parsing
        and not pd.api.types.is_datetime64_any_dtype(queue.loc[:, col])
    )]

    # add _raw suffix
    rename_dict = dict(zip(date_cols, [col + '_raw' for col in date_cols]))
    queue.rename(columns=rename_dict, inplace=True)

    for date_col, raw_col in rename_dict.items():
        new_dates = parse_dates(queue.loc[:, raw_col])
        # set obviously bad values to null
        # This is designed to catch values improperly encoded by Excel to 1899 or 1900
        bad = new_dates.dt.year <= (EXCEL_EPOCH_ORIGIN.year + 1)
        new_dates.loc[bad] = pd.NaT
        queue.loc[:, date_col] = new_dates
    return


def replace_value_with_count_validation(df: pd.DataFrame, col: str, val_to_replace: Any, replacement: Any, expected_count: int) -> None:
    """Manually replace values, but with a minimal form of validation to guard against future changes.

    Args:
        ser (pd.Series): the series in question
        val_to_replace (Any): value to replace
        replacement (Any): replacement value
        expected_count (int): known number of replacements to make

    Raises:
        ValueError: if expected count of replacements does not match observed count
    """
    matches = df.loc[:, col] == val_to_replace
    observed_count = matches.sum()
    if observed_count != expected_count:
        raise ValueError(
            f"Expected count ({expected_count}) of {val_to_replace} "
            f"does not match observed count ({observed_count})"
        )

    df.loc[matches, col] = replacement
    return
