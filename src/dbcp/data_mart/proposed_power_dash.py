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
    county_opp = county_opp.merge(county_fips_df, on="county_id_fips", copy=False)
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


def _get_existing_plants(engine: sa.engine.Engine) -> pd.DataFrame:
    """Aggregate existing plants by county and resource to a long-format table.

    Args:
        engine (sa.engine.Engine): database connection.

    Returns:
        pd.DataFrame: county-resource level table of existing capacity.
    """
    # see last SELECT statement for output columns
    query = """
    WITH
    county_aggs as (
        SELECT
        county_id_fips,
        (CASE
            WHEN technology_description = 'Batteries' THEN 'Battery Storage'
            WHEN technology_description = 'Offshore Wind Turbine' THEN 'Offshore Wind'
            WHEN fuel_type_code_pudl = 'waste' THEN 'other'
            ELSE fuel_type_code_pudl
        END
        ) as resource,
        sum(capacity_mw) as capacity_mw
        FROM data_warehouse.mcoe
        WHERE operational_status = 'existing'
        GROUP BY 1, 2
        ORDER BY 1, 2
    ),
    w_county_names as (
    select
        cfip.county_name,
        cfip.state_id_fips,
        cfip.county_id_fips,
        agg.resource,
        agg.capacity_mw
    from county_aggs as agg
    left join data_warehouse.county_fips as cfip
        on agg.county_id_fips = cfip.county_id_fips
    ),
    w_names as (
        SELECT
            sfip.state_name as state,
            agg.county_name as county,
            agg.state_id_fips,
            agg.county_id_fips,
            agg.resource,
            agg.capacity_mw
        from w_county_names as agg
        left join data_warehouse.state_fips as sfip
            on agg.state_id_fips = sfip.state_id_fips
    )
    select
        agg.state,
        agg.county,
        agg.state_id_fips,
        agg.county_id_fips,
        agg.resource,
        agg.capacity_mw,
        ncsl.permitting_type
    from w_names as agg
    left join data_warehouse.ncsl_state_permitting as ncsl
        on agg.state_id_fips = ncsl.state_id_fips
    ;
    """
    df = pd.read_sql(query, engine)
    # harmonize categories with ISO queue
    resource_map = {
        "gas": "Natural Gas",
        "wind": "Onshore Wind",
        "hydro": "Hydro",
        "oil": "Oil",
        "nuclear": "Nuclear",
        "coal": "Coal",
        "other": "Other",
        "solar": "Solar",
        "Battery Storage": "Battery Storage",
        "Offshore Wind": "Offshore Wind",
    }
    df.loc[:, "resource"] = df.loc[:, "resource"].map(resource_map)
    return df


def _get_proposed_plants(engine: sa.engine.Engine) -> pd.DataFrame:
    """Aggregate proposed (ISO active queue) plants by county and resource to a long-format table.

    Args:
        engine (sa.engine.Engine): database connection.

    Returns:
        pd.DataFrame: county-resource level table of proposed capacity.
    """
    # see last SELECT statement for output columns
    query = """
    WITH
    active_loc as (
        select
            proj.project_id,
            loc.county_id_fips
        from data_warehouse.iso_projects_2021 as proj
        left join data_warehouse.iso_locations_2021 as loc
            on loc.project_id = proj.project_id
        where proj.queue_status = 'active'
    ),
    county_aggs as (
        select
            loc.county_id_fips,
            (CASE res.resource_clean
                WHEN 'Pumped Storage' THEN 'Other'
                WHEN 'Waste Heat' THEN 'Other'
                WHEN 'Geothermal' THEN 'Other'
                WHEN 'Other Storage' THEN 'Other'
                ELSE res.resource_clean
            END
            ) as resource,
            sum(res.capacity_mw) as capacity_mw,
            count(distinct loc.project_id) as project_count
        from active_loc as loc
        left join data_warehouse.iso_resource_capacity_2021 as res
            on res.project_id = loc.project_id
        group by 1, 2
        order by 1, 2
    ),
    w_county_names as (
    select
        cfip.county_name,
        cfip.state_id_fips,
        cfip.county_id_fips,
        agg.resource,
        agg.capacity_mw,
        agg.project_count
    from county_aggs as agg
    left join data_warehouse.county_fips as cfip
        on agg.county_id_fips = cfip.county_id_fips
    ),
    w_names as (
        SELECT
            sfip.state_name as state,
            agg.county_name as county,
            agg.state_id_fips,
            agg.county_id_fips,
            agg.resource,
            agg.capacity_mw,
            agg.project_count
        from w_county_names as agg
        left join data_warehouse.state_fips as sfip
            on agg.state_id_fips = sfip.state_id_fips
    )
    select
        agg.state,
        agg.county,
        agg.state_id_fips,
        agg.county_id_fips,
        agg.resource,
        agg.capacity_mw,
        agg.project_count,
        ncsl.permitting_type
    from w_names as agg
    left join data_warehouse.ncsl_state_permitting as ncsl
        on agg.state_id_fips = ncsl.state_id_fips
    ;
    """
    df = pd.read_sql(query, engine)
    return df


def _add_has_ordinance_column(
    *, county_df: pd.DataFrame, local_opp: pd.DataFrame
) -> pd.DataFrame:
    """Add boolean column has_ordinance to indicate presence of local opposition.

    Args:
        county_df (pd.DataFrame): dataframe with county FIPS codes
        local_opp (pd.DataFrame): dataframe of local ordinances

    Returns:
        pd.DataFrame: county_df plus a new column
    """
    out = county_df.merge(
        local_opp[["has_ordinance", "county_id_fips"]],
        how="left",
        on="county_id_fips",
        copy=False,
        validate="m:1",
    )
    out.loc[:, "has_ordinance"] = out.loc[:, "has_ordinance"].fillna(False)
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
        "proposed_power_dash_existing_plants": _get_existing_plants(engine),
        "proposed_power_dash_proposed_plants": _get_proposed_plants(engine),
    }
    for key in [
        "proposed_power_dash_existing_plants",
        "proposed_power_dash_proposed_plants",
    ]:
        dfs[key] = _add_has_ordinance_column(
            county_df=dfs[key], local_opp=dfs["proposed_power_dash_local_opp"]
        )
    dfs["proposed_power_dash_local_opp"] = _add_permitting_type_column(
        dfs["proposed_power_dash_local_opp"], engine=engine
    )

    return dfs
