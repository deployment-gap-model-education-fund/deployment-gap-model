"""Module to create a county-level table for DBCP to use in spreadsheet tools."""
from typing import Optional, Sequence

import numpy as np
import pandas as pd
import sqlalchemy as sa

from dbcp.constants import PUDL_LATEST_YEAR
from dbcp.data_mart.co2_dashboard import (
    _estimate_existing_co2e,
    _get_existing_plant_fuel_data,
    _get_plant_location_data,
    _transfrom_plant_location_data,
)
from dbcp.data_mart.fossil_infrastructure_projects import (
    create_data_mart as create_fossil_infra_data_mart,
)
from dbcp.data_mart.helpers import (
    CountyOpposition,
    _get_county_fips_df,
    _get_state_fips_df,
    _subset_db_columns,
)
from dbcp.data_mart.projects import create_data_mart as create_iso_data_mart
from dbcp.helpers import download_pudl_data, get_sql_engine


def _get_existing_plant_attributes(engine: sa.engine.Engine) -> pd.DataFrame:
    # get plant_id, fuel_type, capacity_mw
    query = """
    WITH
    plant_fuel_aggs as (
        SELECT
            plant_id_eia,
            (CASE
                WHEN technology_description = 'Batteries' THEN 'Battery Storage'
                WHEN technology_description = 'Offshore Wind Turbine' THEN 'Offshore Wind'
                WHEN fuel_type_code_pudl = 'waste' THEN 'other'
                ELSE fuel_type_code_pudl
            END
            ) as resource,
            sum(net_generation_mwh) as net_gen_by_fuel,
            sum(capacity_mw) as capacity_by_fuel,
            max(operating_date) as max_operating_date
        from data_warehouse.mcoe
        where operational_status = 'existing'
        group by 1, 2
    ),
    plant_capacity as (
        SELECT
            plant_id_eia,
            sum(capacity_by_fuel) as capacity_mw
        from plant_fuel_aggs
        group by 1
    ),
    all_aggs as (
        SELECT
            *
        from plant_fuel_aggs as pfuel
        LEFT JOIN plant_capacity as pcap
        USING (plant_id_eia)
    )
    -- select fuel type with the largest generation (with capacity as tiebreaker)
    -- https://stackoverflow.com/questions/3800551/select-first-row-in-each-group-by-group/7630564
    -- NOTE: this is not appropriate for fields that require aggregation, hence CTEs above
    SELECT DISTINCT ON (plant_id_eia)
        plant_id_eia,
        resource,
        -- net_gen_by_fuel for debugging
        max_operating_date,
        capacity_mw
    from all_aggs
    ORDER BY plant_id_eia, net_gen_by_fuel DESC NULLS LAST, capacity_by_fuel DESC NULLS LAST
    ;
    """
    df = pd.read_sql(query, engine)
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


def _get_existing_fossil_plant_co2e_estimates(
    pudl_engine: sa.engine.Engine,
) -> pd.DataFrame:
    gen_fuel_923 = _get_existing_plant_fuel_data(pudl_engine)
    plant_co2e = _estimate_existing_co2e(gen_fuel_923)
    return plant_co2e


def _get_existing_plant_locations(
    pudl_engine: sa.engine.Engine,
    postgres_engine: sa.engine.Engine,
    state_fips_table: Optional[pd.DataFrame] = None,
    county_fips_table: Optional[pd.DataFrame] = None,
):
    if state_fips_table is None:
        state_fips_table = _get_state_fips_df(postgres_engine)
    if county_fips_table is None:
        county_fips_table = _get_county_fips_df(postgres_engine)
    plant_locations = _get_plant_location_data(pudl_engine)
    plant_locations = _transfrom_plant_location_data(
        plant_locations, state_table=state_fips_table, county_table=county_fips_table
    )
    return plant_locations


def _get_existing_plants(
    pudl_engine: sa.engine.Engine,
    postgres_engine: sa.engine.Engine,
    state_fips_table: Optional[pd.DataFrame] = None,
    county_fips_table: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    plants = _get_existing_plant_attributes(engine=postgres_engine)
    co2e = _get_existing_fossil_plant_co2e_estimates(pudl_engine=pudl_engine)
    locations = _get_existing_plant_locations(
        pudl_engine=pudl_engine,
        postgres_engine=postgres_engine,
        state_fips_table=state_fips_table,
        county_fips_table=county_fips_table,
    )
    plants = plants.merge(co2e, how="left", on="plant_id_eia", copy=False)
    plants = plants.merge(locations, how="left", on="plant_id_eia", copy=False)
    return plants


def _existing_plants_counties(
    pudl_engine: sa.engine.Engine,
    postgres_engine: sa.engine.Engine,
    state_fips_table: Optional[pd.DataFrame] = None,
    county_fips_table: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    plants = _get_existing_plants(
        pudl_engine=pudl_engine,
        postgres_engine=postgres_engine,
        state_fips_table=state_fips_table,
        county_fips_table=county_fips_table,
    )
    grp = plants.groupby(["county_id_fips", "resource"])
    aggs = grp.agg(
        {
            "co2e_tonnes_per_year": "sum",
            "capacity_mw": "sum",
            "plant_id_eia": "count",
        }
    )
    aggs.loc[:, "co2e_tonnes_per_year"].replace(
        0, np.nan, inplace=True
    )  # sums of 0 are simply unmodeled
    aggs["facility_type"] = "power plant"
    aggs["status"] = "existing"
    aggs.reset_index(inplace=True)
    aggs.rename(
        columns={
            "plant_id_eia": "facility_count",
            "resource": "resource_or_sector",
        },
        inplace=True,
    )
    return aggs


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
    grp = infra.groupby(["county_id_fips", "industry_sector"])
    aggs = grp.agg(
        {
            "co2e_tonnes_per_year": "sum",
            "pm2_5_tonnes_per_year": "sum",
            "nox_tonnes_per_year": "sum",
            "project_id": "count",
        }
    )
    aggs.loc[:, "co2e_tonnes_per_year"].replace(
        0, np.nan, inplace=True
    )  # sums of 0 are simply unmodeled
    aggs["facility_type"] = "fossil infrastructure"
    aggs["status"] = "proposed"
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
    iso = create_iso_data_mart(engine)["iso_projects_long_format"]

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
    grp = iso.groupby(["county_id_fips", "resource_clean"])
    aggs = grp.agg(
        {
            "co2e_tonnes_per_year": "sum",
            "capacity_mw": "sum",
            "project_id": "count",
        }
    )
    aggs.loc[:, "co2e_tonnes_per_year"].replace(
        0, np.nan, inplace=True
    )  # sums of 0 are simply unmodeled
    aggs["facility_type"] = "power plant"
    aggs["status"] = "proposed"
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


def _convert_long_to_wide(long_format: pd.DataFrame) -> pd.DataFrame:

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
    #return out[col_order]


def create_data_mart(
    engine: Optional[sa.engine.Engine] = None,
    pudl_engine: Optional[sa.engine.Engine] = None,
) -> pd.DataFrame:
    """Create the counties datamart dataframe."""
    postgres_engine = engine
    if postgres_engine is None:
        postgres_engine = get_sql_engine()
    if pudl_engine is None:
        pudl_data_path = download_pudl_data()
        pudl_engine = sa.create_engine(
            f"sqlite:////{pudl_data_path}/pudl_data/sqlite/pudl.sqlite"
        )

    ncsl = _get_ncsl_wind_permitting_df(postgres_engine)
    all_counties = _get_county_fips_df(postgres_engine)
    all_states = _get_state_fips_df(postgres_engine)
    iso = _iso_projects_counties(postgres_engine)
    infra = _fossil_infrastructure_counties(postgres_engine)
    existing = _existing_plants_counties(
        pudl_engine=pudl_engine,
        postgres_engine=postgres_engine,
        state_fips_table=all_states,
        county_fips_table=all_counties,
    )

    # model local opposition
    aggregator = CountyOpposition(
        engine=postgres_engine, county_fips_df=all_counties, state_fips_df=all_states
    )
    combined_opp = aggregator.agg_to_counties(include_state_policies=True)

    # join it all
    out = pd.concat([iso, existing, infra], axis=0, ignore_index=True)
    out = out.merge(combined_opp, on="county_id_fips", how="left")
    out["state_id_fips"] = out["county_id_fips"].str[:2]
    out = out.merge(ncsl, on="state_id_fips", how="left", validate="m:1")
    # use canonical state and county names
    out = out.merge(
        all_states[["state_name", "state_id_fips"]],
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
    rename_dict = {
        "geocoded_locality_name": "ordinance_jurisdiction_name",
        "geocoded_locality_type": "ordinance_jurisdiction_type",
        "earliest_year_mentioned": "ordinance_earliest_year_mentioned",
        "description": "state_permitting_text",
        "permitting_type": "state_permitting_type",
        "state_name": "state",
        "county_name": "county",
    }
    out = out.rename(columns=rename_dict)
    col_order = [
        "state_id_fips",
        "county_id_fips",
        "state",
        "county",
        "facility_type",
        "resource_or_sector",
        "status",
        "facility_count",
        "capacity_mw",
        "co2e_tonnes_per_year",
        "pm2_5_tonnes_per_year",
        "nox_tonnes_per_year",
        "has_ordinance",
        "ordinance_jurisdiction_name",
        "ordinance_jurisdiction_type",
        "ordinance",
        "ordinance_earliest_year_mentioned",
        "state_permitting_type",
        "state_permitting_text",
    ]
    return out.loc[:, col_order]
