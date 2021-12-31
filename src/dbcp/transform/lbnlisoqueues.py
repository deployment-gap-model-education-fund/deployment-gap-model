"""Functions to transform EIP Infrastructure tables."""

import logging
from typing import Dict, Any, List

import pandas as pd

from dbcp.transform.helpers import EXCEL_EPOCH_ORIGIN, parse_dates, normalize_multicolumns_to_rows
from pudl.helpers import add_fips_ids as _add_fips_ids

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
    # standardize columns between queues
    completed_projects.loc[:, 'interconnection_status_lbnl'] = 'IA Executed'
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
    # standardize values between queues
    withdrawn_projects.loc[:, 'interconnection_status_lbnl'].replace(
        'Executed', 'IA Executed', inplace=True)

    return withdrawn_projects


def transform(lbnl_raw_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Transform LBNL ISO Queues dataframes.

    Args:
        lbnl_raw_dfs: Dictionary of the raw extracted data for each table.

    Returns:
        lbnl_transformed_dfs: Dictionary of the transformed tables.
    """

    lbnl_transformed_dfs = {name: df.copy()
                            for name, df in lbnl_raw_dfs.items()}
    _set_global_project_ids(lbnl_transformed_dfs)

    lbnl_transform_functions = {
        "active_iso_queue_projects": active_iso_queue_projects,
        "completed_iso_queue_projects": completed_iso_queue_projects,
        "withdrawn_iso_queue_projects": withdrawn_iso_queue_projects,
    }

    for table_name, transform_func in lbnl_transform_functions.items():
        logger.info(f"LBNL ISO Queues: Transforming {table_name} table.")
        lbnl_transformed_dfs[table_name] = transform_func(
            lbnl_transformed_dfs[table_name])
    lbnl_normalized_dfs = normalize_lbnl_dfs(lbnl_transformed_dfs)
    # data enrichment
    lbnl_normalized_dfs['iso_locations'] = add_fips_codes(
        lbnl_normalized_dfs['iso_locations'])
    lbnl_normalized_dfs['iso_for_tableau'] = denormalize(lbnl_normalized_dfs)
    return lbnl_normalized_dfs


def _set_global_project_ids(lbnl_dfs: Dict[str, pd.DataFrame]) -> None:
    """Reindex (in place) the three LBNL queues with IDs unique between all three dataframes.

    Args:
        lbnl_dfs (Dict[str, pd.DataFrame]): dict with the three LBNL queue dataframes
    """
    previous_idx_max = 0
    for df in lbnl_dfs.values():
        new_idx = pd.RangeIndex(previous_idx_max, len(
            df) + previous_idx_max, name='project_id')
        df.set_index(new_idx, inplace=True)
        previous_idx_max = new_idx.max() + 1
    return


def parse_date_columns(queue: pd.DataFrame) -> None:
    """Identify date columns and parse them to pd.Timestamp.

    Original (unparsed) date columns are preserved but with the suffix '_raw'.

    Args:
        queue (pd.DataFrame): an LBNL ISO queue dataframe
    """
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
        # This is designed to catch NaN values improperly encoded by Excel to 1899 or 1900
        bad = new_dates.dt.year.isin({1899, 1900})
        new_dates.loc[bad] = pd.NaT
        queue.loc[:, date_col] = new_dates
    return


def replace_value_with_count_validation(df: pd.DataFrame, col: str, val_to_replace: Any, replacement: Any, expected_count: int) -> None:
    """Manually replace values, but with a minimal form of validation to guard against future changes.

    Args:
        df (pd.DataFrame): the source dataframe
        col (str): the name of the column containing values to replace
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


def _normalize_resource_capacity(lbnl_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Pull out the awkward one-to-many columns (type_1, capacity_1, type_2, capacity_2)
    to a separate dataframe.

    Args:
        lbnl_df (pd.DataFrame): LBNL ISO queue dataframe

    Returns:
        Dict[str, pd.DataFrame]: dict with the projects and multivalues split into two dataframes
    """
    if 'capacity_mw_resource_3' in lbnl_df.columns:  # only active projects
        n_multicolumns = 3
    else:
        n_multicolumns = 2
    attr_columns = {
        'resource': ['resource_type_' + str(n) for n in range(1, n_multicolumns + 1)],
        'capacity_mw': ['capacity_mw_resource_' + str(n) for n in range(1, n_multicolumns + 1)]
    }
    resource_capacity_df = normalize_multicolumns_to_rows(lbnl_df,
                                                          attribute_columns_dict=attr_columns,
                                                          preserve_original_names=False,
                                                          dropna=True)
    combined_cols: List[str] = sum(attr_columns.values(), start=[])
    project_df = lbnl_df.drop(columns=combined_cols)

    return {'resource_capacity_df': resource_capacity_df, 'project_df': project_df}


def _normalize_location(lbnl_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Pull out the awkward one-to-many columns (county_1, county_2, etc)
    to a separate dataframe.

    Args:
        lbnl_df (pd.DataFrame): LBNL ISO queue dataframe

    Returns:
        Dict[str, pd.DataFrame]: dict with the projects and locations split into two dataframes
    """
    if 'county_3' in lbnl_df.columns:  # only active projects are multivalued
        county_cols = ['county_' + str(n) for n in range(1, 4)]
        location_df = normalize_multicolumns_to_rows(lbnl_df,
                                                     attribute_columns_dict={
                                                         'county': county_cols},
                                                     preserve_original_names=False,
                                                     dropna=True)
        location_df = location_df.merge(
            lbnl_df.loc[:, 'state'], on='project_id', validate='m:1')

        project_df = lbnl_df.drop(columns=county_cols+['state'])
    else:
        location_df = lbnl_df.loc[:, ['state', 'county']].reset_index()
        project_df = lbnl_df.drop(columns=['state', 'county'])

    location_df.dropna(subset=['state', 'county'], how='all', inplace=True)
    return {'location_df': location_df, 'project_df': project_df}


def normalize_lbnl_dfs(lbnl_transformed_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Normalize one-to-many columns and combine the three queues.

    Args:
        lbnl_transformed_dfs (Dict[str, pd.DataFrame]): the LBNL ISO queue dataframes

    Returns:
        Dict[str, pd.DataFrame]: the combined queues, normalized into projects, locations, and resource_capacity
    """
    resource_capacity_dfs = [_normalize_resource_capacity(df)
                             for df in lbnl_transformed_dfs.values()
                             ]
    resource_capacity_df = pd.concat(
        [df_dict['resource_capacity_df'] for df_dict in resource_capacity_dfs],
        ignore_index=True)
    location_dfs = [_normalize_location(df_dict['project_df'])
                    for df_dict in resource_capacity_dfs]
    location_df = pd.concat([df_dict['location_df']
                            for df_dict in location_dfs],
                            ignore_index=True)
    project_df = pd.concat([df_dict['project_df']
                           for df_dict in location_dfs])  # keep project_id index
    return {
        'iso_projects': project_df,
        'iso_locations': location_df,
        'iso_resource_capacity': resource_capacity_df,
    }


def add_fips_codes(location_df: pd.DataFrame) -> pd.DataFrame:
    """Add columns with state and county Federal Information Processing System (FIPS) ID codes.

    Args:
        location_df (pd.DataFrame): normalized lbnl ISO queue location df

    Returns:
        pd.DataFrame: copy of location_df with two new columns containing FIPS codes
    """
    with_fips = _add_fips_ids(
        location_df.fillna({'state': ''}),
        state_col='state',
        county_col='county',
    )
    # fix about 50 independent cities with wrong name order
    nan_fips = with_fips.loc[with_fips['county_id_fips'].isna(), :].copy()
    nan_fips.loc[:, 'county'] = nan_fips.loc[:, 'county'].str.replace(
        '^City of (.+)',
        lambda x: x.group(1) + ' City',
        regex=True
    )
    nan_fips = _add_fips_ids(nan_fips)
    with_fips.loc[:, 'county_id_fips'].fillna(
        nan_fips['county_id_fips'], inplace=True)
    return with_fips


def denormalize(lbnl_normalized_dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    # TODO: this should be a view in SQL
    loc_proj = lbnl_normalized_dfs['iso_locations'].merge(
        lbnl_normalized_dfs['iso_projects'], on='project_id', how='outer', validate='m:1')
    all_proj = loc_proj.merge(
        lbnl_normalized_dfs['iso_resource_capacity'], on='project_id', how='outer', validate="m:m")
    return all_proj
