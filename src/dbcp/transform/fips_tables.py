"""Tranform raw FIPS tables to a database-ready form."""
import logging
from typing import Dict, Sequence

import pandas as pd

from dbcp.schemas import TABLE_SCHEMAS

logger = logging.getLogger(__name__)


def county_fips(counties: pd.DataFrame) -> pd.DataFrame:
    """
    Apply transformations to county FIPS table.

    Args:
        counties: raw county_fips table.

    Returns:
        transformed county_fips table.
    """
    counties = counties.copy()
    counties = _dedupe_keep_shortest_name(counties, idx_cols=['statefp', 'countyfp'])

    # make 5 digit FIPS
    counties['county_id_fips'] = counties['statefp'] + counties['countyfp']

    rename_dict = {'statefp': 'state_id_fips', 'name': 'county_name', }
    counties = (counties.rename(columns=rename_dict).drop(columns='countyfp'))

    # Validate schema
    counties = TABLE_SCHEMAS["county_fips"].validate(counties)
    assert "object" not in counties.dtypes

    return counties


def state_fips(states: pd.DataFrame) -> pd.DataFrame:
    """
    Apply transformations to county FIPS table.

    Args:
        states: raw county_fips table.

    Returns:
        transformed county_fips table.
    """
    states = states.copy()
    states = _dedupe_keep_shortest_name(states, idx_cols=['fips', ])

    rename_dict = {'fips': 'state_id_fips',
                   'name': 'state_name', 'postal': 'state_abbrev'}
    states = states.rename(columns=rename_dict)

    # Validate schema
    states = TABLE_SCHEMAS["state_fips"].validate(states)
    assert "object" not in states.dtypes

    return states


def transform(fips_tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Transform state and county FIPS dataframes.

    Args:
        fips_tables: Dictionary of the raw extracted data for each FIPS table.

    Returns:
        transformed_fips_tables: Dictionary of the transformed tables.
    """
    transformed_fips_tables = {}

    transform_functions = {
        "county_fips": county_fips,
        "state_fips": state_fips,
    }

    for table_name, transform_func in transform_functions.items():
        logger.info(f"FIPS tables: Transforming {table_name} table.")

        table_df = fips_tables[table_name].copy()
        transformed_fips_tables[table_name] = transform_func(table_df)

    return transformed_fips_tables


def _dedupe_keep_shortest_name(df: pd.DataFrame, idx_cols: Sequence[str], name_col: str = 'name') -> pd.DataFrame:
    """Several states and counties have multiple entries with short- and long-form names. This function removes all but the shortest.

    Example: 'Rhode Island' vs 'Rhode Island and Providence Plantations'

    Args:
        df (pd.DataFrame): input dataframe of states or counties
        idx_cols (Sequence[str]): column(s) comprising a (compound) key. Also determines output sort order.
        name_col (str, optional): column used for sorting based on length. Defaults to 'name'.

    Returns:
        pd.DataFrame: deduplicated copy of input dataframe, sorted by idx_cols
    """
    sorted_idx = df[name_col].str.len().sort_values(ascending=True).index
    sorted_ = df.loc[sorted_idx, :]
    deduped = sorted_.drop_duplicates(
        subset=idx_cols, keep='first').sort_values(idx_cols)
    return deduped
