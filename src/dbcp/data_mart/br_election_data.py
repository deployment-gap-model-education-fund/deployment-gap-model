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


def _create_county_commission_election_info(engine: sa.engine.Engine) -> pd.DataFrame:
    """Create a data mart of county commission elections."""
    # Each row in this query describe an election in a county.
    # I select the maximum frequency and reference_year because they describes a position, not an election.
    query = """
        SELECT
            county_id_fips,
            election_id,
            county,
            election_name,
            election_day,
            SUM(number_of_seats) AS total_n_of_seats,
            COUNT(position_id) AS total_n_races,
            STRING_AGG(position_name, ',') AS all_race_names,
            MAX(reference_year) AS reference_year,
            MAX(frequency) AS frequency
        FROM
            data_mart.br_election_data
        WHERE
            tier > 2
            AND is_judicial = FALSE
            AND normalized_position_id IN (910,
                912)
        GROUP BY
            1,
            2,
            3,
            4,
            5
        ORDER BY
            4,
            1;
    """

    with engine.connect() as con:
        county_commission_election_info = pd.read_sql_query(query, con)
    return county_commission_election_info


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
    dfs["county_commission_election_info"] = _create_county_commission_election_info(
        engine
    )
    return dfs
