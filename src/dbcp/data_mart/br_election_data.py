"""Module to create a denormalized table for the Ballot Ready Election Data."""
from typing import Optional

import pandas as pd
import sqlalchemy as sa

from dbcp.helpers import get_sql_engine


def _create_br_election_data_mart(engine: sa.engine.Engine) -> pd.DataFrame:
    """Create a minimally transformed data mart table for Ballot Ready data."""
    with engine.connect() as con:
        df = pd.read_sql_table("br_election_data", schema="data_warehouse", con=con)
    rename_map = {"raw_county": "county", "raw_state": "state"}
    df = df.rename(columns=rename_map)
    return df


def _create_county_commission_election_info(
    br_election_data: pd.DataFrame,
) -> pd.DataFrame:
    """Create a data mart of county commission elections."""
    pass


def create_data_mart(
    engine: Optional[sa.engine.Engine] = None,
    pudl_engine: Optional[sa.engine.Engine] = None,
) -> dict[str, pd.DataFrame]:
    """Create final output table.

    Args:
        engine (Optional[sa.engine.Engine], optional): postgres engine. Defaults to None.

    Returns:
        pd.DataFrame: table for data mart
    """
    if engine is None:
        engine = get_sql_engine()

    dfs = {}

    dfs["br_election_data"] = _create_br_election_data_mart(engine)
    # dfs["county_commission_election_info"] = _create_county_commission_election_info(dfs["br_election_data"])
    return dfs
