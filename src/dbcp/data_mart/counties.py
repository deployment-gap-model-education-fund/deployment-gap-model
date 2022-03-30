"""Module to create a county-level table for DBCP to use in spreadsheet tools."""
from typing import Sequence, Optional

import sqlalchemy as sa
import pandas as pd
from dbcp.helpers import get_sql_engine
from dbcp.constants import PUDL_LATEST_YEAR


def _subset_db_columns(columns: Sequence[str], table: str, engine: sa.engine.Engine)-> pd.DataFrame:
    query = f"SELECT {', '.join(columns)} FROM {table}"
    df = pd.read_sql(query, engine)
    return df

def _get_iso_location_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        'project_id',
        #'county',  # drop raw name in favor of canonical FIPS name
        #'state',  # drop raw state in favor of canonical FIPS name
        #'state_id_fips',
        'county_id_fips',
        #'locality_name',  # drop detailed location info for simplicity
        #'locality_type',  # drop detailed location info for simplicity
        #'containing_county',  # drop geocoded name in favor of canonical FIPS name
    ]
    db = 'dbcp.iso_locations'
    
    simple_location_df = _subset_db_columns(cols, db, engine)
    # If multiple counties, just pick the first one. This is simplistic but there are only 26/13259 (0.2%)
    simple_location_df = simple_location_df.groupby('project_id', as_index=False).nth(0)
    return simple_location_df

def _get_iso_resource_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        'capacity_mw',
        #'project_class',  # will model this according to client wants
        'project_id',
        #'resource',  # drop raw name in favor of resource_clean
        #'resource_class',  # will model this according to client wants
        'resource_clean',
    ]
    db = 'dbcp.iso_resource_capacity'
    df = _subset_db_columns(cols, db, engine)
    return df

def _get_iso_project_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        #'date_operational',  # not needed for higher level aggregates
        #'date_proposed',  # not needed for higher level aggregates
        #'date_proposed_raw',  # not needed for higher level aggregates
        #'date_withdrawn',  # not needed for higher level aggregates
        #'date_withdrawn_raw',  # not needed for higher level aggregates
        #'days_in_queue',  # not needed for higher level aggregates
        #'developer',  # not needed for higher level aggregates
        #'entity',  # not needed for higher level aggregates
        #'interconnection_status_lbnl',  # not needed for higher level aggregates
        #'interconnection_status_raw',  # not needed for higher level aggregates
        #'point_of_interconnection',  # not needed for higher level aggregates
        'project_id',
        #'project_name',  # not needed for higher level aggregates
        'queue_date',  # needed for filtering
        #'queue_date_raw',  # not needed for higher level aggregates
        #'queue_id',  # not needed for higher level aggregates
        'queue_status',  # needed for filtering
        #'queue_year',  # year info is containted in queue_date
        'region',
        #'resource_type_lbnl',  # just use clean types for simplicity
        #'utility',  # not needed for higher level aggregates
        #'withdrawl_reason',  # not needed for higher level aggregates
        #'year_operational',  # year info is contained in date_operational
        #'year_proposed',  # year info is contained in date_proposed
        #'year_withdrawn',  # year info is contained in date_withdrawn
    ]
    db = 'dbcp.iso_projects'
    df = _subset_db_columns(cols, db, engine)
    return df

def _get_pudl_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        #'associated_combined_heat_power',
        #'balancing_authority_code_eia',
        #'balancing_authority_name_eia',
        #'bga_source',
        #'bypass_heat_recovery',
        #'capacity_factor',
        'capacity_mw',
        #'carbon_capture',
        #'city',
        #'cofire_fuels',
        #'county',
        'county_id_fips',
        #'current_planned_operating_date',
        #'data_source',
        #'deliver_power_transgrid',
        #'distributed_generation',
        #'duct_burners',
        #'energy_source_1_transport_1',
        #'energy_source_1_transport_2',
        #'energy_source_1_transport_3',
        #'energy_source_2_transport_1',
        #'energy_source_2_transport_2',
        #'energy_source_2_transport_3',
        #'energy_source_code_1',
        #'energy_source_code_2',
        #'energy_source_code_3',
        #'energy_source_code_4',
        #'energy_source_code_5',
        #'energy_source_code_6',
        #'ferc_cogen_status',
        #'ferc_exempt_wholesale_generator',
        #'ferc_small_power_producer',
        #'fluidized_bed_tech',
        #'fuel_cost_from_eiaapi',
        #'fuel_cost_per_mmbtu',
        #'fuel_cost_per_mwh',
        'fuel_type_code_pudl',
        #'fuel_type_count',
        #'generator_id',
        #'grid_voltage_2_kv',
        #'grid_voltage_3_kv',
        #'grid_voltage_kv',
        #'heat_rate_mmbtu_mwh',
        #'iso_rto_code',
        #'latitude',
        #'longitude',
        #'minimum_load_mw',
        #'multiple_fuels',
        #'nameplate_power_factor',
        #'net_generation_mwh',
        'operating_date',
        #'operating_switch',
        #'operational_status',
        #'operational_status_code',
        #'original_planned_operating_date',
        #'other_combustion_tech',
        #'other_modifications_date',
        #'other_planned_modifications',
        #'owned_by_non_utility',
        #'ownership_code',
        #'planned_derate_date',
        #'planned_energy_source_code_1',
        #'planned_modifications',
        #'planned_net_summer_capacity_derate_mw',
        #'planned_net_summer_capacity_uprate_mw',
        #'planned_net_winter_capacity_derate_mw',
        #'planned_net_winter_capacity_uprate_mw',
        #'planned_new_capacity_mw',
        #'planned_new_prime_mover_code',
        #'planned_repower_date',
        #'planned_retirement_date',
        #'planned_uprate_date',
        #'plant_id_eia',
        #'plant_id_pudl',
        #'plant_name_eia',
        #'previously_canceled',
        #'primary_purpose_id_naics',
        #'prime_mover_code',
        #'pulverized_coal_tech',
        #'reactive_power_output_mvar',
        #'report_date',
        #'retirement_date',
        #'rto_iso_lmp_node_id',
        #'rto_iso_location_wholesale_reporting_id',
        #'sector_id_eia',
        #'sector_name_eia',
        #'solid_fuel_gasification',
        #'startup_source_code_1',
        #'startup_source_code_2',
        #'startup_source_code_3',
        #'startup_source_code_4',
        #'state',
        #'state_id_fips',
        #'stoker_tech',
        #'street_address',
        #'subcritical_tech',
        #'summer_capacity_estimate',
        #'summer_capacity_mw',
        #'summer_estimated_capability_mw',
        #'supercritical_tech',
        #'switch_oil_gas',
        #'syncronized_transmission_grid',
        'technology_description',  # needed for energy storage
        #'time_cold_shutdown_full_load_code',
        #'timezone',
        #'topping_bottoming_code',
        #'total_fuel_cost',
        #'total_mmbtu',
        #'turbines_inverters_hydrokinetics',
        #'turbines_num',
        #'ultrasupercritical_tech',
        #'unit_id_pudl',
        #'uprate_derate_completed_date',
        #'uprate_derate_during_year',
        #'utility_id_eia',
        #'utility_id_pudl',
        #'utility_name_eia',
        #'winter_capacity_estimate',
        #'winter_capacity_mw',
        #'winter_estimated_capability_mw',
        #'zip_code',
    ]
    db = 'dbcp.mcoe'
    # I don't use _subset_db_columns because of extra filtering requirements
    query = f"""select {', '.join(cols)} from {db}
    where operational_status = 'existing'
    and report_date = date('{PUDL_LATEST_YEAR}-01-01')"""
    df = pd.read_sql(query, engine)
    return df

def _get_ncsl_wind_permitting_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        'description',
        #'link',  # too detailed?
        'permitting_type',
        #'state',  # only need FIPS, state name comes from elsewhere
        'state_id_fips',
    ]
    db = 'dbcp.ncsl_state_permitting'
    df = _subset_db_columns(cols, db, engine)
    return df

def _get_local_opposition_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        #'containing_county',  # only need FIPS, names come from elsewhere
        'county_id_fips',
        'earliest_year_mentioned',
        #'latest_year_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
        'locality_name',
        'locality_type',
        #'n_years_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
        'ordinance',
        #'raw_locality_name',  # drop raw name in favor of canonical one
        #'raw_state_name',  # drop raw name in favor of canonical one
        #'state_id_fips',  # will join on 5-digit county FIPS, which includes state
    ]
    db = 'dbcp.local_ordinance'
    df = _subset_db_columns(cols, db, engine)
    return df

def _get_state_opposition_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        'earliest_year_mentioned',
        #'latest_year_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
        #'n_years_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
        'policy',
        #'raw_state_name',  # drop raw name in favor of canonical one
        'state_id_fips',
    ]
    db = 'dbcp.state_policy'
    df = _subset_db_columns(cols, db, engine)
    return df

def _get_county_fips_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = ['*']
    db = 'dbcp.county_fips'
    df = _subset_db_columns(cols, db, engine)
    return df


def _get_state_fips_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = ['*']
    db = 'dbcp.state_fips'
    df = _subset_db_columns(cols, db, engine)
    return df

def _filter_state_opposition(state_df: pd.DataFrame) -> pd.DataFrame:
    """Drop states that repealed their policies or whose policy was pro-renewables instead of anti-renewables.

    Args:
        state_df (pd.DataFrame): state policy dataframe

    Returns:
        pd.DataFrame: filtered copy of the input state policy dataframe
    """
    fips_codes_to_drop = {'23', '36'} # Maine (repealed), New York (pro-RE)
    not_dropped = ~state_df.loc[:, 'state_id_fips'].isin(fips_codes_to_drop)
    filtered_state_df = state_df.loc[not_dropped,:].copy()
    return filtered_state_df

def _represent_state_opposition_as_counties(state_df: pd.DataFrame, county_fips_df: pd.DataFrame, state_fips_df: pd.DataFrame) -> pd.DataFrame:
    """Downscale state policies to look like county-level ordinances at each county in the respective state.

    To make concatenation easier, the output dataframe imitates the columns of the local ordinance table.

    Args:
        state_df (pd.DataFrame): state opposition dataframe
        county_fips_df (pd.DataFrame): master table of all counties
        state_fips_df (pd.DataFrame): master table of all states

    Returns:
        pd.DataFrame: fanned out state policy dataframe
    """
    # fan out
    states_as_counties = state_df.merge(county_fips_df.loc[:, ['county_id_fips', 'state_id_fips']], on='state_id_fips', how='left')
    
    # replicate local opposition columns
    # locality_name
    states_as_counties = states_as_counties.merge(state_fips_df.loc[:, ['state_name', 'state_id_fips']], on='state_id_fips', how='left')
    # locality_type
    states_as_counties['locality_type'] = 'state'
    rename_dict = {
        'state_name': 'locality_name',
        'policy': 'ordinance',
    }
    states_as_counties = states_as_counties.rename(columns=rename_dict).drop(columns=['state_id_fips'])
    return states_as_counties

def _agg_local_ordinances_to_counties(ordinances: pd.DataFrame) -> pd.DataFrame:
    """Force the local ordinance table to have 1 row = 1 county. Only 8/92 counties have multiple ordinances.

    This is necessary for joining into the ISO project table. ISO projects are only located by county.
    Aggregation method:
    * take min of earliest_year_mentioned
    * if only one locality_name, use it. Otherwise replace with "multiple"
    * same with 'locality type'
    * concatenate the rdinances, each with locality_name prefix, eg "Great County: <ordinance>\nSmall Town: <ordinance>"

    Value Counts of # ordinances per county (as of 3/14/2022):
    1 ord    84 counties
    2 ord     6 counties
    3 ord     1 county
    4 ord     1 county

    Args:
        ordinances (pd.DataFrame): local ordinance dataframe

    Returns:
        pd.DataFrame: aggregated local ordinance dataframe
    """
    dupe_counties = ordinances.duplicated(subset='county_id_fips', keep=False)
    dupes = ordinances.loc[dupe_counties,:].copy()
    not_dupes = ordinances.loc[~dupe_counties,:].copy()
    
    dupes['ordinance'] = dupes['locality_name'] + ': ' + dupes['ordinance'] + '\n'
    grp = dupes.groupby('county_id_fips')

    years = grp['earliest_year_mentioned'].min()

    n_unique = grp[['locality_name', 'locality_type']].nunique()
    localities = grp[['locality_name', 'locality_type']].nth(0).mask(n_unique > 1, other='multiple')

    descriptions = grp['ordinance'].sum().str.strip()

    agg_dupes = pd.concat([years, localities, descriptions], axis=1).reset_index()
    recombined = pd.concat([not_dupes, agg_dupes], axis=0, ignore_index=True).sort_values('county_id_fips')
    assert not recombined.duplicated(subset='county_id_fips').any()

    return recombined


def _get_and_join_iso_tables(engine: Optional[sa.engine.Engine]=None) -> pd.DataFrame:
    if engine is None:
        engine = get_sql_engine()
    project_df = _get_iso_project_df(engine)
    location_df = _get_iso_location_df(engine)
    resource_df = _get_iso_resource_df(engine)
    
    combined = (resource_df
        .merge(project_df, on='project_id', how='outer')
        .merge(location_df, on='project_id', how='outer')
        )
    return combined.reset_index(drop=True)


def _agg_iso_projects_to_counties(iso_df: pd.DataFrame) -> pd.DataFrame:
    # filter for active projects
    is_active = iso_df.loc[:,'queue_status'] == 'active'
    projects = iso_df.loc[is_active,:].copy()
    
    # Calulate renewable/fossil aggregates
    # define categories
    renewable_fossil_dict = {
        'Solar': 'renewable',
        #'Battery Storage': '',
        'Onshore Wind': 'renewable',
        'Natural Gas': 'fossil',
        'Offshore Wind': 'renewable',
        #'Other Storage': '',
        #'Other': '',
        #'Hydro': '',  # remove by client request
        #'Nuclear': '',
        #'Pumped Storage': '',
        'Geothermal': 'renewable',
        'Coal': 'fossil',
        #'Waste Heat': '',
    }
    
    projects['renew_fossil'] = projects.loc[:,'resource_clean'].map(renewable_fossil_dict)
    category_groups = projects.groupby(['county_id_fips', 'renew_fossil'])
    category_aggs = category_groups['capacity_mw'].sum().to_frame()
    # count ignores missing capacity values, so have to join size() in separately
    category_aggs = category_aggs.join(category_groups.size().rename('count_plants').to_frame())
    category_aggs = category_aggs.unstack()

    # calculate fuel breakout aggregates
    projects['breakout_fuels'] = projects.loc[:,'resource_clean'].replace({'Natural Gas': 'Gas'})
    projects.loc[:, 'breakout_fuels'] = projects.loc[:, 'breakout_fuels'].str.lower()
    fuels_to_breakout = {'solar', 'onshore wind', 'offshore wind', 'gas', 'battery storage'}
    to_breakout = projects.loc[:, 'breakout_fuels'].isin(fuels_to_breakout)
    projects = projects.loc[to_breakout,:]

    fuel_groups = projects.groupby(['county_id_fips', 'breakout_fuels'])
    per_fuel_aggs = fuel_groups['capacity_mw'].sum().to_frame()
    # count ignores missing capacity values, so have to join size() in separately
    per_fuel_aggs = per_fuel_aggs.join(fuel_groups.size().rename('count_plants').to_frame())
    per_fuel_aggs = per_fuel_aggs.unstack()

    output = pd.concat([per_fuel_aggs, category_aggs], axis=1)
    output.columns = output.columns.swaplevel()

    return output


def _agg_pudl_to_counties(pudl_df: pd.DataFrame) -> pd.DataFrame:
    plants = pudl_df.copy()
    # Calulate renewable/fossil aggregates
    # define categories
    renewable_fossil_dict = {
        'gas': 'fossil',
        #'hydro': '',  # remove by client request
        'oil': 'fossil',
        #'nuclear': '',
        'coal': 'fossil',
        #'other': '',
        'wind': 'renewable',
        'solar': 'renewable',
        #'waste': '',
    }
    
    plants['renew_fossil'] = plants.loc[:,'fuel_type_code_pudl'].map(renewable_fossil_dict)
    category_groups = plants.groupby(['county_id_fips', 'renew_fossil'])
    category_aggs = category_groups['capacity_mw'].sum().to_frame()
    # count ignores missing capacity values, so have to join size() in separately
    category_aggs = category_aggs.join(category_groups.size().rename('count_plants').to_frame())
    category_aggs = category_aggs.unstack()

    # calculate fuel breakout aggregates
    # relabel battery storage (this ignores pumped hydro)
    plants['breakout_fuels'] = plants.loc[:,'fuel_type_code_pudl'].replace({'wind': 'onshore wind'})  # copy
    is_battery = plants.loc[:, 'technology_description'] == 'Batteries'
    plants.loc[is_battery, 'breakout_fuels'] = 'battery storage'

    fuels_to_breakout = {'solar', 'onshore wind', 'gas', 'battery storage', 'coal'}
    to_breakout = plants.loc[:, 'breakout_fuels'].isin(fuels_to_breakout)
    plants = plants.loc[to_breakout,:]

    fuel_groups = plants.groupby(['county_id_fips', 'breakout_fuels'])
    per_fuel_aggs = fuel_groups['capacity_mw'].sum().to_frame()
    # count ignores missing capacity values, so have to join size() in separately
    per_fuel_aggs = per_fuel_aggs.join(fuel_groups.size().rename('count_plants').to_frame())
    per_fuel_aggs = per_fuel_aggs.unstack()

    output = pd.concat([per_fuel_aggs, category_aggs], axis=1)
    output.columns = output.columns.swaplevel()

    return output


def _add_derived_columns(mart: pd.DataFrame) -> pd.DataFrame:
    out = mart.copy()
    out['has_ordinance'] = out['ordinance'].notna()
    
    return out

def _flatten_multiindex(multi_index: pd.MultiIndex, prefix: str) -> pd.Index:
    flattened = ['_'.join([levels[0], prefix, *levels[1:]]).replace(' ', '_') for levels in multi_index]
    return pd.Index(flattened)

def make_county_data_mart_table(engine: Optional[sa.engine.Engine]=None) -> pd.DataFrame:
    if engine is None:
        engine = get_sql_engine()
    iso = _get_and_join_iso_tables(engine)
    pudl = _get_pudl_df(engine)  # already filtered on operational_status and report_date
    ncsl = _get_ncsl_wind_permitting_df(engine)
    local_opp = _get_local_opposition_df(engine)
    state_opp = _get_state_opposition_df(engine)
    all_counties = _get_county_fips_df(engine)
    all_states = _get_state_fips_df(engine)

    # model local opposition
    filtered_state_opp = _filter_state_opposition(state_opp)
    states_to_counties = _represent_state_opposition_as_counties(filtered_state_opp, county_fips_df=all_counties, state_fips_df=all_states)
    combined_opp = pd.concat([local_opp, states_to_counties], axis=0)
    combined_opp = _agg_local_ordinances_to_counties(combined_opp)
    
    # aggregate
    iso_aggs = _agg_iso_projects_to_counties(iso)
    iso_aggs.columns = _flatten_multiindex(iso_aggs.columns, prefix='proposed')
    existing_aggs = _agg_pudl_to_counties(pudl)
    existing_aggs.columns = _flatten_multiindex(existing_aggs.columns, prefix='existing')
    recent_build = pudl.query(f'operating_date >= "{PUDL_LATEST_YEAR}-01-01"')
    recent_aggs = _agg_pudl_to_counties(recent_build)
    recent_aggs.columns = _flatten_multiindex(recent_aggs.columns, prefix=f'built_in_{PUDL_LATEST_YEAR}')

    # join it all
    out = pd.concat([iso_aggs, existing_aggs, recent_aggs], axis=1)
    out = out.join(combined_opp.set_index('county_id_fips'), how='left')
    out = out.reset_index()
    out['state_id_fips'] = out['county_id_fips'].str[:2]
    out = out.merge(ncsl, on='state_id_fips', how='left', validate='m:1')
    # use canonical state and county names
    out = out.merge(all_states[['state_abbrev', 'state_id_fips']], on='state_id_fips', how='left', validate='m:1')
    out = out.merge(all_counties[['county_name', 'county_id_fips']], on='county_id_fips', how='left', validate='m:1')

    out = _add_derived_columns(out)
    
    assert len(out) <= len(all_counties)
    rename_dict = {
        'locality_name': 'ordinance_jurisdiction_name',
        'locality_type': 'ordinance_jurisdiction_type',
        'earliest_year_mentioned': 'ordinance_earliest_year_mentioned',
        'description': 'state_permitting_text',
        'permitting_type': 'state_permitting_type',
        'state_abbrev': 'state',
        'county_name': 'county',
    }
    out = out.rename(columns=rename_dict)

    col_order = [
        'state_id_fips',
        'county_id_fips',
        'state',
        'county',
        'fossil_built_in_2020_capacity_mw',
        'fossil_built_in_2020_count_plants',
        'fossil_existing_capacity_mw',
        'fossil_existing_count_plants',
        'fossil_proposed_capacity_mw',
        'fossil_proposed_count_plants',
        'renewable_built_in_2020_capacity_mw',
        'renewable_built_in_2020_count_plants',
        'renewable_existing_capacity_mw',
        'renewable_existing_count_plants',
        'renewable_proposed_capacity_mw',
        'renewable_proposed_count_plants',
        'battery_storage_built_in_2020_capacity_mw',
        'battery_storage_built_in_2020_count_plants',
        'battery_storage_existing_capacity_mw',
        'battery_storage_existing_count_plants',
        'battery_storage_proposed_capacity_mw',
        'battery_storage_proposed_count_plants',
        #'coal_built_in_2020_capacity_mw',  # drop
        #'coal_built_in_2020_count_plants',  # drop
        'coal_existing_capacity_mw',
        'coal_existing_count_plants',
        'gas_built_in_2020_capacity_mw',
        'gas_built_in_2020_count_plants',
        'gas_existing_capacity_mw',
        'gas_existing_count_plants',
        'gas_proposed_capacity_mw',
        'gas_proposed_count_plants',
        'solar_built_in_2020_capacity_mw',
        'solar_built_in_2020_count_plants',
        'solar_existing_capacity_mw',
        'solar_existing_count_plants',
        'solar_proposed_capacity_mw',
        'solar_proposed_count_plants',
        'onshore_wind_built_in_2020_capacity_mw',
        'onshore_wind_built_in_2020_count_plants',
        'onshore_wind_existing_capacity_mw',
        'onshore_wind_existing_count_plants',
        'onshore_wind_proposed_capacity_mw',
        'onshore_wind_proposed_count_plants',
        'offshore_wind_proposed_capacity_mw',
        'offshore_wind_proposed_count_plants',
        'has_ordinance',
        'ordinance_jurisdiction_name',
        'ordinance_jurisdiction_type',
        'ordinance',
        'ordinance_earliest_year_mentioned',
        'state_permitting_type',
        'state_permitting_text',
    ]
    return out[col_order]
