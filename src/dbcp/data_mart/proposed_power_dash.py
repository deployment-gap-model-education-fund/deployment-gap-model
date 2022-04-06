"""Module to create a county-level table of local ordinances for use in Tableau map layers.

The motivating dashboard for this table is the proposed vs existing power dashboard."""

from typing import Optional, Dict

import sqlalchemy as sa
import pandas as pd
from dbcp.helpers import get_sql_engine
from dbcp.data_mart.helpers import CountyOpposition, _get_county_fips_df, _get_state_fips_df


def local_opposition(
    county_fips_df: Optional[pd.DataFrame] = None,
    state_fips_df: Optional[pd.DataFrame] = None,
    engine: Optional[sa.engine.Engine] = None,
) -> pd.DataFrame:
    if engine is None:
        engine = get_sql_engine()
    if county_fips_df is None:
        county_fips_df = _get_county_fips_df(engine)
    if state_fips_df is None:
        state_fips_df = _get_state_fips_df(engine)

    aggregator = CountyOpposition(engine=engine, county_fips_df=county_fips_df, state_fips_df=state_fips_df)
    county_opp = aggregator.agg_to_counties(include_state_policies=True)

    # bring in county names and state_id_fips
    county_opp = county_opp.merge(county_fips_df, on='county_id_fips', copy=False)
    county_opp = county_opp.merge(state_fips_df, on='state_id_fips',
                                  copy=False)  # bring in state name and abbreviation
    rename_dict = {
        'geocoded_locality_name': 'ordinance_jurisdiction_name',
        'geocoded_locality_type': 'ordinance_jurisdiction_type',
        'earliest_year_mentioned': 'ordinance_earliest_year_mentioned',
        'state_name': 'state',
        'county_name': 'county',
    }
    county_opp.rename(columns=rename_dict, inplace=True)
    return county_opp


def _get_existing_plants(engine: sa.engine.Engine) -> pd.DataFrame:
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
        FROM dbcp.mcoe
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
    left join dbcp.county_fips as cfip
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
        left join dbcp.state_fips as sfip
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
    left join dbcp.ncsl_state_permitting as ncsl
        on agg.state_id_fips = ncsl.state_id_fips
    ;
    """
    df = pd.read_sql(query, engine)
    # harmonize categories with ISO queue
    resource_map = {
        'gas': 'Natural Gas',
        'wind': 'Onshore Wind',
        'hydro': 'Hydro',
        'oil': 'Oil',
        'nuclear': 'Nuclear',
        'coal': 'Coal',
        'other': 'Other',
        'solar': 'Solar',
        'Battery Storage': 'Battery Storage',
        'Offshore Wind': 'Offshore Wind',
    }
    df.loc[:, 'resource'] = df.loc[:, 'resource'].map(resource_map)
    return df


def _get_proposed_plants(engine: sa.engine.Engine) -> pd.DataFrame:
    query = """
    WITH
    active_loc as (
        select
            proj.project_id,
            loc.county_id_fips
        from dbcp.iso_projects as proj
        left join dbcp.iso_locations as loc
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
            sum(res.capacity_mw) as capacity_mw
        from active_loc as loc
        left join dbcp.iso_resource_capacity as res
            on res.project_id = loc.project_id
        where capacity_mw is not NULL
        group by 1, 2
        order by 1, 2
    ),
    w_county_names as (
    select
        cfip.county_name,
        cfip.state_id_fips,
        cfip.county_id_fips,
        agg.resource,
        agg.capacity_mw
    from county_aggs as agg
    left join dbcp.county_fips as cfip
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
        left join dbcp.state_fips as sfip
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
    left join dbcp.ncsl_state_permitting as ncsl
        on agg.state_id_fips = ncsl.state_id_fips
    ;
    """
    df = pd.read_sql(query, engine)
    return df


def make_dashboard_tables(engine: Optional[sa.engine.Engine] = None) -> Dict[str, pd.DataFrame]:
    if engine is None:
        engine = get_sql_engine()
    dfs = {
        'local_opp': local_opposition(engine=engine),
        'existing_plants': _get_existing_plants(engine),
        'proposed_plants': _get_proposed_plants(engine),
    }
    return dfs
