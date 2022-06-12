"""Module to create a county-level table for DBCP to use in spreadsheet tools."""
from typing import Optional, Sequence

import pandas as pd
import sqlalchemy as sa

from dbcp.constants import PUDL_LATEST_YEAR
from dbcp.data_mart.fossil_infrastructure_projects import (
    create_data_mart as create_fossil_infra_data_mart,
)
from dbcp.data_mart.projects import create_data_mart as create_iso_data_mart
from dbcp.helpers import get_sql_engine
from dbcp.data_mart.helpers import CountyOpposition, _get_county_fips_df, _get_state_fips_df, _subset_db_columns


def _get_pudl_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        # 'associated_combined_heat_power',
        # 'balancing_authority_code_eia',
        # 'balancing_authority_name_eia',
        # 'bga_source',
        # 'bypass_heat_recovery',
        # 'capacity_factor',
        "capacity_mw",
        # 'carbon_capture',
        # 'city',
        # 'cofire_fuels',
        # 'county',
        "county_id_fips",
        # 'current_planned_operating_date',
        # 'data_source',
        # 'deliver_power_transgrid',
        # 'distributed_generation',
        # 'duct_burners',
        # 'energy_source_1_transport_1',
        # 'energy_source_1_transport_2',
        # 'energy_source_1_transport_3',
        # 'energy_source_2_transport_1',
        # 'energy_source_2_transport_2',
        # 'energy_source_2_transport_3',
        # 'energy_source_code_1',
        # 'energy_source_code_2',
        # 'energy_source_code_3',
        # 'energy_source_code_4',
        # 'energy_source_code_5',
        # 'energy_source_code_6',
        # 'ferc_cogen_status',
        # 'ferc_exempt_wholesale_generator',
        # 'ferc_small_power_producer',
        # 'fluidized_bed_tech',
        # 'fuel_cost_from_eiaapi',
        # 'fuel_cost_per_mmbtu',
        # 'fuel_cost_per_mwh',
        "fuel_type_code_pudl",
        # 'fuel_type_count',
        # 'generator_id',
        # 'grid_voltage_2_kv',
        # 'grid_voltage_3_kv',
        # 'grid_voltage_kv',
        # 'heat_rate_mmbtu_mwh',
        # 'iso_rto_code',
        # 'latitude',
        # 'longitude',
        # 'minimum_load_mw',
        # 'multiple_fuels',
        # 'nameplate_power_factor',
        # 'net_generation_mwh',
        "operating_date",
        # 'operating_switch',
        # 'operational_status',
        # 'operational_status_code',
        # 'original_planned_operating_date',
        # 'other_combustion_tech',
        # 'other_modifications_date',
        # 'other_planned_modifications',
        # 'owned_by_non_utility',
        # 'ownership_code',
        # 'planned_derate_date',
        # 'planned_energy_source_code_1',
        # 'planned_modifications',
        # 'planned_net_summer_capacity_derate_mw',
        # 'planned_net_summer_capacity_uprate_mw',
        # 'planned_net_winter_capacity_derate_mw',
        # 'planned_net_winter_capacity_uprate_mw',
        # 'planned_new_capacity_mw',
        # 'planned_new_prime_mover_code',
        # 'planned_repower_date',
        # 'planned_retirement_date',
        # 'planned_uprate_date',
        # 'plant_id_eia',
        # 'plant_id_pudl',
        # 'plant_name_eia',
        # 'previously_canceled',
        # 'primary_purpose_id_naics',
        # 'prime_mover_code',
        # 'pulverized_coal_tech',
        # 'reactive_power_output_mvar',
        # 'report_date',
        # 'retirement_date',
        # 'rto_iso_lmp_node_id',
        # 'rto_iso_location_wholesale_reporting_id',
        # 'sector_id_eia',
        # 'sector_name_eia',
        # 'solid_fuel_gasification',
        # 'startup_source_code_1',
        # 'startup_source_code_2',
        # 'startup_source_code_3',
        # 'startup_source_code_4',
        # 'state',
        # 'state_id_fips',
        # 'stoker_tech',
        # 'street_address',
        # 'subcritical_tech',
        # 'summer_capacity_estimate',
        # 'summer_capacity_mw',
        # 'summer_estimated_capability_mw',
        # 'supercritical_tech',
        # 'switch_oil_gas',
        # 'syncronized_transmission_grid',
        "technology_description",  # needed for energy storage
        # 'time_cold_shutdown_full_load_code',
        # 'timezone',
        # 'topping_bottoming_code',
        # 'total_fuel_cost',
        # 'total_mmbtu',
        # 'turbines_inverters_hydrokinetics',
        # 'turbines_num',
        # 'ultrasupercritical_tech',
        # 'unit_id_pudl',
        # 'uprate_derate_completed_date',
        # 'uprate_derate_during_year',
        # 'utility_id_eia',
        # 'utility_id_pudl',
        # 'utility_name_eia',
        # 'winter_capacity_estimate',
        # 'winter_capacity_mw',
        # 'winter_estimated_capability_mw',
        # 'zip_code',
    ]
    db = "data_warehouse.mcoe"
    # I don't use _subset_db_columns because of extra filtering requirements
    query = f"""select {', '.join(cols)} from {db}
    where operational_status = 'existing'
    and report_date = date('{PUDL_LATEST_YEAR}-01-01')"""
    df = pd.read_sql(query, engine)
    return df


def _fossil_infrastructure_counties(engine: sa.engine.Engine) -> pd.DataFrame:
    # Avoid db dependency order by recreating the df.
    # Could also make an orchestration script.
    infra = create_fossil_infra_data_mart(engine)

    # equivalent SQL query that I translated to pandas to avoid dependency
    # on the data_mart schema (which doesn't yet exist when this function runs)
    """
    SELECT
        county_id_fips,
        industry_sector as resource_or_sector,
        count(project_id) as facility_count,
        sum(co2e_tonnes_per_year) as co2e_tonnes_per_year,
        sum(pm2_5_tonnes_per_year) as pm2_5_tonnes_per_year,
        sum(nox_tonnes_per_year) as nox_tonnes_per_year,
        'power plant' as facility_type,
        'proposed' as status
    from data_mart.fossil_infrastructure_projects
    group by 1, 2
    ;
    """
    grp = infra.groupby(["county_id_fips", 'industry_sector'])
    aggs = grp.agg(
        {
            "co2e_tonnes_per_year": "sum",
            "pm2_5_tonnes_per_year": "sum",
            "nox_tonnes_per_year": "sum",
            "project_id": "count",
        }
    )
    aggs['facility_type'] = 'fossil infrastructure'
    aggs['status'] = 'proposed'
    aggs.reset_index(inplace=True)
    aggs.rename(
        columns={
            "project_id": "facility_count",
            "industry_sector": "resource_or_sector",
        },
        inplace=True,
    )
    return aggs


def _iso_projects_counties(engine: sa.engine.Engine) -> pd.DataFrame:
    # Avoid db dependency order by recreating the df.
    # Could also make an orchestration script.
    iso = create_iso_data_mart(engine)['iso_projects_long_format']

    # equivalent SQL query that I translated to pandas to avoid dependency
    # on the data_mart schema (which doesn't yet exist when this function runs)
    """
    SELECT
        county_id_fips,
        resource_clean as resource_or_sector,
        count(project_id) as facility_count,
        sum(co2e_tonnes_per_year) as co2e_tonnes_per_year,
        sum(capacity_mw) as capacity_mw,
        'power plant' as facility_type,
        'proposed' as status
    from data_mart.iso_projects_long_format
    group by 1, 2
    ;
    """
    grp = iso.groupby(["county_id_fips", 'resource_clean'])
    aggs = grp.agg(
        {
            "co2e_tonnes_per_year": "sum",
            "capacity_mw": "sum",
            "project_id": "count",
        }
    )
    aggs['facility_type'] = 'power plant'
    aggs['status'] = 'proposed'
    aggs.reset_index(inplace=True)
    aggs.rename(
        columns={
            "project_id": "facility_count",
            "resource_clean": "resource_or_sector",
        },
        inplace=True,
    )
    return aggs


def _get_ncsl_wind_permitting_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        "description",
        # 'link',  # too detailed?
        "permitting_type",
        # 'raw_state_name',  # drop raw name in favor of canonical one
        "state_id_fips",
    ]
    db = "data_warehouse.ncsl_state_permitting"
    df = _subset_db_columns(cols, db, engine)
    return df


def _agg_iso_projects_to_counties(iso_df: pd.DataFrame) -> pd.DataFrame:
    # filter for active projects
    is_active = iso_df.loc[:, "queue_status"] == "active"
    projects = iso_df.loc[is_active, :].copy()

    # Calulate renewable/fossil aggregates
    # define categories
    renewable_fossil_dict = {
        "Solar": "renewable",
        # 'Battery Storage': '',
        "Onshore Wind": "renewable",
        "Natural Gas": "fossil",
        "Offshore Wind": "renewable",
        # 'Other Storage': '',
        # 'Other': '',
        # 'Hydro': '',  # remove by client request
        # 'Nuclear': '',
        # 'Pumped Storage': '',
        "Geothermal": "renewable",
        "Coal": "fossil",
        # 'Waste Heat': '',
    }

    projects["renew_fossil"] = projects.loc[:, "resource_clean"].map(
        renewable_fossil_dict
    )
    category_groups = projects.groupby(["county_id_fips", "renew_fossil"])
    category_aggs = category_groups["capacity_mw"].sum().to_frame()
    # count ignores missing capacity values, so have to join size() in separately
    category_aggs = category_aggs.join(
        category_groups.size().rename("count_plants").to_frame()
    )
    category_aggs = category_aggs.unstack()

    # calculate fuel breakout aggregates
    projects["breakout_fuels"] = projects.loc[:, "resource_clean"].replace(
        {"Natural Gas": "Gas"}
    )
    projects.loc[:, "breakout_fuels"] = projects.loc[:, "breakout_fuels"].str.lower()
    fuels_to_breakout = {
        "solar",
        "onshore wind",
        "offshore wind",
        "gas",
        "battery storage",
    }
    to_breakout = projects.loc[:, "breakout_fuels"].isin(fuels_to_breakout)
    projects = projects.loc[to_breakout, :]

    fuel_groups = projects.groupby(["county_id_fips", "breakout_fuels"])
    per_fuel_aggs = fuel_groups["capacity_mw"].sum().to_frame()
    # count ignores missing capacity values, so have to join size() in separately
    per_fuel_aggs = per_fuel_aggs.join(
        fuel_groups.size().rename("count_plants").to_frame()
    )
    per_fuel_aggs = per_fuel_aggs.unstack()

    output = pd.concat([per_fuel_aggs, category_aggs], axis=1)
    output.columns = output.columns.swaplevel()

    return output


def _agg_pudl_to_counties(pudl_df: pd.DataFrame) -> pd.DataFrame:
    plants = pudl_df.copy()
    # Calulate renewable/fossil aggregates
    # define categories
    renewable_fossil_dict = {
        "gas": "fossil",
        # 'hydro': '',  # remove by client request
        "oil": "fossil",
        # 'nuclear': '',
        "coal": "fossil",
        # 'other': '',
        "wind": "renewable",
        "solar": "renewable",
        # 'waste': '',
    }

    plants["renew_fossil"] = plants.loc[:, "fuel_type_code_pudl"].map(
        renewable_fossil_dict
    )
    category_groups = plants.groupby(["county_id_fips", "renew_fossil"])
    category_aggs = category_groups["capacity_mw"].sum().to_frame()
    # count ignores missing capacity values, so have to join size() in separately
    category_aggs = category_aggs.join(
        category_groups.size().rename("count_plants").to_frame()
    )
    category_aggs = category_aggs.unstack()

    # calculate fuel breakout aggregates
    # relabel battery storage (this ignores pumped hydro)
    plants["breakout_fuels"] = plants.loc[:, "fuel_type_code_pudl"].replace(
        {"wind": "onshore wind"}
    )  # copy
    is_battery = plants.loc[:, "technology_description"] == "Batteries"
    plants.loc[is_battery, "breakout_fuels"] = "battery storage"

    fuels_to_breakout = {"solar", "onshore wind", "gas", "battery storage", "coal"}
    to_breakout = plants.loc[:, "breakout_fuels"].isin(fuels_to_breakout)
    plants = plants.loc[to_breakout, :]

    fuel_groups = plants.groupby(["county_id_fips", "breakout_fuels"])
    per_fuel_aggs = fuel_groups["capacity_mw"].sum().to_frame()
    # count ignores missing capacity values, so have to join size() in separately
    per_fuel_aggs = per_fuel_aggs.join(
        fuel_groups.size().rename("count_plants").to_frame()
    )
    per_fuel_aggs = per_fuel_aggs.unstack()

    output = pd.concat([per_fuel_aggs, category_aggs], axis=1)
    output.columns = output.columns.swaplevel()

    return output


def _add_derived_columns(mart: pd.DataFrame) -> pd.DataFrame:
    out = mart.copy()
    out["has_ordinance"] = out["ordinance"].notna()

    return out


def _flatten_multiindex(multi_index: pd.MultiIndex, prefix: str) -> pd.Index:
    flattened = [
        "_".join([levels[0], prefix, *levels[1:]]).replace(" ", "_")
        for levels in multi_index
    ]
    return pd.Index(flattened)


def create_data_mart(engine: Optional[sa.engine.Engine] = None) -> pd.DataFrame:
    """Create the counties datamart dataframe."""
    if engine is None:
        engine = get_sql_engine()
    pudl = _get_pudl_df(
        engine
    )  # already filtered on operational_status and report_date
    ncsl = _get_ncsl_wind_permitting_df(engine)
    all_counties = _get_county_fips_df(engine)
    all_states = _get_state_fips_df(engine)
    iso = _iso_projects_counties(engine)
    infra = _fossil_infrastructure_counties(engine)

    # model local opposition
    aggregator = CountyOpposition(
        engine=engine, county_fips_df=all_counties, state_fips_df=all_states
    )
    combined_opp = aggregator.agg_to_counties(include_state_policies=True)

    # aggregate
    iso_aggs = _agg_iso_projects_to_counties(iso)
    iso_aggs.columns = _flatten_multiindex(iso_aggs.columns, prefix="proposed")
    existing_aggs = _agg_pudl_to_counties(pudl)
    existing_aggs.columns = _flatten_multiindex(
        existing_aggs.columns, prefix="existing"
    )
    recent_build = pudl.query(f'operating_date >= "{PUDL_LATEST_YEAR}-01-01"')
    recent_aggs = _agg_pudl_to_counties(recent_build)
    recent_aggs.columns = _flatten_multiindex(
        recent_aggs.columns, prefix=f"built_in_{PUDL_LATEST_YEAR}"
    )

    # join it all
    out = pd.concat([iso_aggs, existing_aggs, recent_aggs], axis=1)
    out = out.join(
        [combined_opp.set_index("county_id_fips"), infra.set_index("county_id_fips")],
        how="left",
    )
    out = out.reset_index()
    out["state_id_fips"] = out["county_id_fips"].str[:2]
    out = out.merge(ncsl, on="state_id_fips", how="left", validate="m:1")
    # use canonical state and county names
    out = out.merge(
        all_states[["state_abbrev", "state_id_fips"]],
        on="state_id_fips",
        how="left",
        validate="m:1",
    )
    out = out.merge(
        all_counties[["county_name", "county_id_fips"]],
        on="county_id_fips",
        how="left",
        validate="m:1",
    )

    out = _add_derived_columns(out)

    assert len(out) <= len(all_counties)
    rename_dict = {
        "geocoded_locality_name": "ordinance_jurisdiction_name",
        "geocoded_locality_type": "ordinance_jurisdiction_type",
        "earliest_year_mentioned": "ordinance_earliest_year_mentioned",
        "description": "state_permitting_text",
        "permitting_type": "state_permitting_type",
        "state_abbrev": "state",
        "county_name": "county",
    }
    out = out.rename(columns=rename_dict)

    col_order = [
        "state_id_fips",
        "county_id_fips",
        "state",
        "county",
        "fossil_built_in_2020_capacity_mw",
        "fossil_built_in_2020_count_plants",
        "fossil_existing_capacity_mw",
        "fossil_existing_count_plants",
        "fossil_proposed_capacity_mw",
        "fossil_proposed_count_plants",
        "renewable_built_in_2020_capacity_mw",
        "renewable_built_in_2020_count_plants",
        "renewable_existing_capacity_mw",
        "renewable_existing_count_plants",
        "renewable_proposed_capacity_mw",
        "renewable_proposed_count_plants",
        "battery_storage_built_in_2020_capacity_mw",
        "battery_storage_built_in_2020_count_plants",
        "battery_storage_existing_capacity_mw",
        "battery_storage_existing_count_plants",
        "battery_storage_proposed_capacity_mw",
        "battery_storage_proposed_count_plants",
        # 'coal_built_in_2020_capacity_mw',  # drop
        # 'coal_built_in_2020_count_plants',  # drop
        "coal_existing_capacity_mw",
        "coal_existing_count_plants",
        "gas_built_in_2020_capacity_mw",
        "gas_built_in_2020_count_plants",
        "gas_existing_capacity_mw",
        "gas_existing_count_plants",
        "gas_proposed_capacity_mw",
        "gas_proposed_count_plants",
        "solar_built_in_2020_capacity_mw",
        "solar_built_in_2020_count_plants",
        "solar_existing_capacity_mw",
        "solar_existing_count_plants",
        "solar_proposed_capacity_mw",
        "solar_proposed_count_plants",
        "onshore_wind_built_in_2020_capacity_mw",
        "onshore_wind_built_in_2020_count_plants",
        "onshore_wind_existing_capacity_mw",
        "onshore_wind_existing_count_plants",
        "onshore_wind_proposed_capacity_mw",
        "onshore_wind_proposed_count_plants",
        "offshore_wind_proposed_capacity_mw",
        "offshore_wind_proposed_count_plants",
        "has_ordinance",
        "ordinance_jurisdiction_name",
        "ordinance_jurisdiction_type",
        "ordinance",
        "ordinance_earliest_year_mentioned",
        "state_permitting_type",
        "state_permitting_text",
        "infra_co2e_tonnes_per_year",
        "infra_pm2_5_tonnes_per_year",
        "infra_nox_tonnes_per_year",
        "infra_count_projects",
        "infra_sector_list",
        "infra_max_pct_low_income",
        "infra_max_pct_people_of_color",
    ]
    return out[col_order]
