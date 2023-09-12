"""Module to create a denormalized table for the Ballot Ready Election Data."""
from typing import Optional

import pandas as pd
import sqlalchemy as sa

from dbcp.helpers import get_sql_engine


def _create_br_election_data_mart(engine: sa.engine.Engine) -> pd.DataFrame:
    """Denormalize the ballot ready entities."""
    pos_county_query = """
    SELECT
        cfips.county_name,
        sfips.state_name,
        br.*
    FROM data_warehouse.br_positions_counties as br
    LEFT JOIN data_warehouse.county_fips as cfips
    USING (county_id_fips)
    LEFT JOIN data_warehouse.state_fips as sfips
    ON sfips.state_id_fips = br.state_id_fips
    """
    with engine.connect() as con:
        br_races = pd.read_sql_table("br_races", con, schema="data_warehouse")
        br_elections = pd.read_sql_table("br_elections", con, schema="data_warehouse")
        br_positions = pd.read_sql_table("br_positions", con, schema="data_warehouse")
        br_positions_counties = pd.read_sql(pos_county_query, con)

    br_election_data = br_races.merge(
        br_elections, how="left", on="election_id", validate="m:1"
    )
    br_election_data = br_election_data.merge(
        br_positions, how="left", on="position_id", validate="m:1"
    )
    br_election_data = br_election_data.merge(
        br_positions_counties, how="left", on="position_id", validate="m:m"
    )
    return br_election_data


def _create_county_commission_elections_long(
    br_election_data: pd.DataFrame,
) -> pd.DataFrame:
    """Create a data mart of county commission elections."""
    commissioner_races = br_election_data.query(
        "tier > 2 & is_judicial == False & normalized_position_id in (910,912)"
    )

    county_name_in_position = commissioner_races.apply(
        lambda row: row.county_name in row.position_name, axis=1
    )
    # I think ballot ready incorrectly geocoded some races. For example,
    # race_id = 1371024: Benewah, Clearwater, and Nez Perce have elections
    # for Latah county comissioners.
    corrected_comissioner_races = commissioner_races[county_name_in_position].copy()

    # Remove city council races for now
    corrected_comissioner_races = corrected_comissioner_races[
        ~corrected_comissioner_races.position_name.str.contains("City Council")
    ]

    # Aggregate
    mode = lambda x: x.value_counts().index[0]  # noqa: E731

    grp_fields = [
        "election_id",
        "county_id_fips",
        "county_name",
        "election_name",
        "election_day",
        "is_primary",
        "is_runoff",
    ]
    agg_funcs = {
        "position_id": "count",
        "number_of_seats": "sum",
        "position_name": lambda x: ",".join(x),
        "frequency": mode,  # frequency describes position, not an election so we select the mode
        "reference_year": mode,  # frequency describes position, not an election so we select the mode
    }

    rename_dict = {
        "number_of_seats": "total_n_seats",
        "position_id": "total_n_races",
        "position_name": "all_race_names",
    }

    comissioner_elections = (
        corrected_comissioner_races.groupby(grp_fields).agg(agg_funcs).reset_index()
    )
    comissioner_elections = comissioner_elections.rename(columns=rename_dict)

    assert ~comissioner_elections.duplicated(
        subset=["county_id_fips", "election_id"]
    ).any(), "County comissioner election primary key is not unique."
    assert (
        comissioner_elections.total_n_seats >= comissioner_elections.total_n_races
    ).all(), "Number of seats should always be greater or equal to number of races in a county."
    return comissioner_elections


def _create_county_commission_elections_wide(
    county_commission_elections_long: pd.DataFrame,
) -> pd.DataFrame:
    """Create a dataframe of county comissioner races where each row is a county with columns for regular, primary and special elections."""
    # Create election_type column to pivot on
    county_commission_elections_long["election_type"] = pd.Series()
    county_commission_elections_long[
        "election_type"
    ] = county_commission_elections_long.election_type.mask(
        county_commission_elections_long.is_primary, "primary"
    )
    county_commission_elections_long[
        "election_type"
    ] = county_commission_elections_long.election_type.mask(
        county_commission_elections_long.is_runoff, "run_off"
    )
    county_commission_elections_long[
        "election_type"
    ] = county_commission_elections_long["election_type"].fillna("general")
    county_commission_elections_long = county_commission_elections_long.drop(
        columns=["is_primary", "is_runoff"]
    )

    # Grab the next upcoming election for each election type and county
    next_county_commission_elections_long = county_commission_elections_long.loc[
        county_commission_elections_long.groupby(["county_id_fips", "election_type"])[
            "election_day"
        ].idxmax()
    ]

    # Pivot and rename columns
    next_county_commission_elections_wide = next_county_commission_elections_long.pivot(
        index=["county_id_fips", "county_name"], columns=["election_type"]
    )

    next_county_commission_elections_wide.columns = (
        next_county_commission_elections_wide.swaplevel(axis=1).columns
    )
    next_county_commission_elections_wide = (
        next_county_commission_elections_wide.sort_index(axis=1, level="election_type")
    )

    next_county_commission_elections_wide.columns = (
        next_county_commission_elections_wide.columns.map("_".join)
    )
    next_county_commission_elections_wide.columns = [
        "next_" + col for col in next_county_commission_elections_wide.columns
    ]

    next_county_commission_elections_wide = (
        next_county_commission_elections_wide.reset_index().convert_dtypes()
    )

    assert (
        next_county_commission_elections_wide.county_id_fips.is_unique
    ), "county_id_fips is not unique!"
    return next_county_commission_elections_wide


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

    county_commission_elections_long = _create_county_commission_elections_long(
        dfs["br_election_data"]
    )
    dfs["county_commission_election_info"] = _create_county_commission_elections_wide(
        county_commission_elections_long
    )
    return dfs
