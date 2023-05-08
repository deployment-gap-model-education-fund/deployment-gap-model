"""Module to create a county-level table of local ordinances for use in Tableau map layers.

The motivating dashboard for this table is the proposed vs existing power dashboard.
"""

from typing import Dict, Optional

import pandas as pd
import sqlalchemy as sa

from dbcp.data_mart.helpers import (
    CountyOpposition,
    _get_county_fips_df,
    _get_state_fips_df,
    _subset_db_columns,
)
from dbcp.helpers import get_sql_engine


def local_opposition(
    county_fips_df: Optional[pd.DataFrame] = None,
    state_fips_df: Optional[pd.DataFrame] = None,
    engine: Optional[sa.engine.Engine] = None,
) -> pd.DataFrame:
    """Combine state and local opposition policies; aggregate to county level.

    Args:
        county_fips_df (Optional[pd.DataFrame], optional): county table from warehouse. Defaults to None.
        state_fips_df (Optional[pd.DataFrame], optional): state table from warehouse. Defaults to None.
        engine (Optional[sa.engine.Engine], optional): database connection. Defaults to None.

    Returns:
        pd.DataFrame: county-level table of anti-renewable policies
    """
    if engine is None:
        engine = get_sql_engine()
    if county_fips_df is None:
        county_fips_df = _get_county_fips_df(engine)
    if state_fips_df is None:
        state_fips_df = _get_state_fips_df(engine)

    aggregator = CountyOpposition(
        engine=engine, county_fips_df=county_fips_df, state_fips_df=state_fips_df
    )
    county_opp = aggregator.agg_to_counties(include_state_policies=False)

    # bring in county names and state_id_fips
    subset = [
        "county_id_fips",
        "state_id_fips",
        "county_name",
    ]
    county_opp = county_opp.merge(
        county_fips_df[subset], on="county_id_fips", copy=False
    )
    county_opp = county_opp.merge(
        state_fips_df, on="state_id_fips", copy=False
    )  # bring in state name and abbreviation
    rename_dict = {
        "geocoded_locality_name": "ordinance_jurisdiction_name",
        "geocoded_locality_type": "ordinance_jurisdiction_type",
        "earliest_year_mentioned": "ordinance_earliest_year_mentioned",
        "state_name": "state",
        "county_name": "county",
    }
    county_opp.rename(columns=rename_dict, inplace=True)
    return county_opp


def _add_ordinance_via_reldi_column(
    *, county_df: pd.DataFrame, local_opp: pd.DataFrame
) -> pd.DataFrame:
    """Add boolean column ordinance_via_reldi to indicate presence of local opposition.

    Args:
        county_df (pd.DataFrame): dataframe with county FIPS codes
        local_opp (pd.DataFrame): dataframe of local ordinances

    Returns:
        pd.DataFrame: county_df plus a new column
    """
    out = county_df.merge(
        local_opp[["ordinance_via_reldi", "county_id_fips"]],
        how="left",
        on="county_id_fips",
        copy=False,
        validate="m:1",
    )
    out.loc[:, "ordinance_via_reldi"] = out.loc[:, "ordinance_via_reldi"].fillna(False)
    return out


def _add_permitting_type_column(
    local_opp: pd.DataFrame, engine: sa.engine.Engine
) -> pd.DataFrame:
    """Add column permitting_type to indicate state/local/hybrid wind permitting.

    Args:
        local_opp (pd.DataFrame): dataframe with local ordinances
        engine (sa.engine.Engine): database connection.

    Returns:
        pd.DataFrame: local_opp plus a new column
    """
    permit_df = _subset_db_columns(
        ["state_id_fips", "permitting_type"],
        "data_warehouse.ncsl_state_permitting",
        engine,
    )
    out = local_opp.merge(
        permit_df, how="left", on="state_id_fips", copy=False, validate="m:1"
    )
    return out


def create_data_mart(
    engine: Optional[sa.engine.Engine] = None,
) -> Dict[str, pd.DataFrame]:
    """API function to create the 3 tables used in the existing vs proposed power dashboard.

    Args:
        engine (Optional[sa.engine.Engine], optional): database connection. Defaults to None.

    Returns:
        Dict[str, pd.DataFrame]: Dataframes with local opposition, existing plants, and proposed plants.
    """
    if engine is None:
        engine = get_sql_engine()
    dfs = {
        "proposed_power_dash_local_opp": local_opposition(engine=engine),
    }
    dfs["proposed_power_dash_local_opp"] = _add_permitting_type_column(
        dfs["proposed_power_dash_local_opp"], engine=engine
    )

    return dfs
