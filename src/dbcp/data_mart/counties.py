"""Module to create a county-level table for DBCP to use in spreadsheet tools."""
from typing import Dict, Optional

import numpy as np
import pandas as pd
import sqlalchemy as sa

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
from dbcp.helpers import get_pudl_engine, get_sql_engine


def _get_env_justice_df(engine: sa.engine.Engine) -> pd.DataFrame:
    query = """
    SELECT
        SUBSTRING("tract_id_fips", 1, 5) as county_id_fips,
        COUNT("tract_id_fips") as total_tracts,
        SUM("is_disadvantaged"::INTEGER) as n_tracts_disadvantaged,
        SUM("all_of_expected_agriculture_loss_and_low_income_and_not_student"::INTEGER) as n_tracts_agriculture_loss_low_income,
        SUM("all_of_expected_building_loss_and_low_income_and_not_students"::INTEGER) as n_tracts_building_loss_low_income,
        SUM("all_of_expected_population_loss_and_low_income_and_not_students"::INTEGER) as n_tracts_population_loss_low_income,
        SUM("all_of_diesel_particulates_and_low_income_and_not_students"::INTEGER) as n_tracts_diesel_particulates_low_income,
        SUM("all_of_energy_burden_and_low_income_and_not_students"::INTEGER) as n_tracts_energy_burden_low_income,
        SUM("all_of_pm2_5_and_low_income_and_not_students"::INTEGER) as n_tracts_pm2_5_low_income,
        SUM("all_of_traffic_and_low_income_and_not_students"::INTEGER) as n_tracts_traffic_low_income,
        SUM("all_of_lead_paint_houses_and_median_home_price_and_low_income_a"::INTEGER) as n_tracts_lead_paint_houses_and_median_home_price_low_income,
        SUM("all_of_housing_burden_and_low_income_and_not_students"::INTEGER) as n_tracts_housing_burden_low_income,
        SUM("all_of_risk_management_plan_proximity_and_low_income_and_not_st"::INTEGER) as n_tracts_risk_management_plan_proximity_low_income,
        SUM("all_of_superfund_proximity_and_low_income_and_not_students"::INTEGER) as n_tracts_superfund_proximity_low_income,
        SUM("all_of_wastewater_and_low_income_and_not_students"::INTEGER) as n_tracts_wastewater_low_income,
        SUM("all_of_asthma_and_low_income_and_not_students"::INTEGER) as n_tracts_asthma_low_income,
        SUM("all_of_heart_disease_and_low_income_and_not_students"::INTEGER) as n_tracts_heart_disease_low_income,
        SUM("all_of_diabetes_and_low_income_and_not_students"::INTEGER) as n_tracts_diabetes_low_income,
        SUM("all_of_local_to_area_income_ratio_and_less_than_high_school_and"::INTEGER) as n_tracts_local_to_area_income_ratio_and_low_high_school,
        SUM("all_of_linguistic_isolation_and_less_than_high_school_and_not_s"::INTEGER) as n_tracts_linguistic_isolation_and_low_high_school,
        SUM("all_of_below_poverty_line_and_less_than_high_school_and_not_stu"::INTEGER) as n_tracts_below_poverty_and_low_high_school,
        SUM("all_of_unemployment_and_less_than_high_school_and_not_students"::INTEGER) as n_tracts_unemployment_and_low_high_school,
        SUM("all_of_hazardous_waste_proximity_and_low_income_and_not_student"::INTEGER) as n_tracts_hazardous_waste_proximity_low_income,
        SUM("all_of_life_expectancy_and_low_income_and_not_students"::INTEGER) as n_tracts_life_expectancy_low_income
    FROM "data_warehouse"."justice40_tracts"
    GROUP BY 1;
    """
    df = pd.read_sql(query, engine)
    return df


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
    long = long_format.copy()
    resources_to_drop = {
        # "Battery Storage",
        # "Solar",
        # "Natural Gas",
        "Nuclear",
        # "Onshore Wind",
        "CSP",
        "Other",
        "Unknown",
        "Geothermal",
        "Other Storage",
        # "Offshore Wind",
        "Hydro",
        "Pumped Storage",
        # "Coal",
        # "Oil",
        # "Liquefied Natural Gas",
        # "Synthetic Fertilizers",
        # "Petrochemicals and Plastics",
    }
    to_drop = long["resource_or_sector"].isin(resources_to_drop)
    long = long.loc[~to_drop, :]

    # prep values that will become part of column names after pivoting
    long.loc[:, "facility_type"] = long.loc[:, "facility_type"].map(
        {"fossil infrastructure": "infra", "power plant": ""}
    )
    long.loc[:, "resource_or_sector"] = long.loc[:, "resource_or_sector"].replace(
        {"Natural Gas": "gas", "Liquefied Natural Gas": "lng"}
    )
    long.loc[:, "resource_or_sector"] = (
        long.loc[:, "resource_or_sector"].str.lower().str.replace(" ", "_", regex=False)
    )

    # pivot
    idx_cols = ["county_id_fips"]
    col_cols = [
        "facility_type",
        "resource_or_sector",
        "status",
    ]
    val_cols = [
        "facility_count",
        "capacity_mw",
        "co2e_tonnes_per_year",
        "pm2_5_tonnes_per_year",
        "nox_tonnes_per_year",
    ]
    county_level_cols = [  # not part of pivot
        "state_id_fips",
        "state",
        "county",
        "has_ordinance",
        "ordinance_jurisdiction_name",
        "ordinance_jurisdiction_type",
        "ordinance",
        "ordinance_earliest_year_mentioned",
        "state_permitting_type",
        "state_permitting_text",
        "total_tracts",
        "n_tracts_disadvantaged",
        "n_tracts_agriculture_loss_low_income",
        "n_tracts_building_loss_low_income",
        "n_tracts_population_loss_low_income",
        "n_tracts_diesel_particulates_low_income",
        "n_tracts_energy_burden_low_income",
        "n_tracts_pm2_5_low_income",
        "n_tracts_traffic_low_income",
        "n_tracts_lead_paint_houses_and_median_home_price_low_income",
        "n_tracts_housing_burden_low_income",
        "n_tracts_risk_management_plan_proximity_low_income",
        "n_tracts_superfund_proximity_low_income",
        "n_tracts_wastewater_low_income",
        "n_tracts_asthma_low_income",
        "n_tracts_heart_disease_low_income",
        "n_tracts_diabetes_low_income",
        "n_tracts_local_to_area_income_ratio_and_low_high_school",
        "n_tracts_linguistic_isolation_and_low_high_school",
        "n_tracts_below_poverty_and_low_high_school",
        "n_tracts_unemployment_and_low_high_school",
        "n_tracts_hazardous_waste_proximity_low_income",
        "n_tracts_life_expectancy_low_income",
    ]
    wide = long.pivot(index=idx_cols, columns=col_cols, values=val_cols)

    wide.columns = wide.columns.rename(
        {None: "measures"}
    )  # value columns: capacity, count, etc
    wide.columns = wide.columns.reorder_levels(col_cols + ["measures"])
    wide.columns = [
        "_".join(col).strip("_") for col in wide.columns
    ]  # flatten multiindex

    # this is equivalent to left joining _get_county_properties()
    county_stuff = long.groupby("county_id_fips")[county_level_cols].first()
    wide = wide.join(county_stuff).reset_index()

    # co2e total
    co2e_cols_to_sum = [
        col for col in wide.columns if col.endswith("co2e_tonnes_per_year")
    ]
    wide["county_total_co2e_tonnes_per_year"] = wide.loc[:, co2e_cols_to_sum].sum(
        axis=1
    )

    # fossil and renewable category totals
    renewables = ["solar", "offshore_wind", "onshore_wind", "battery_storage"]
    fossils = ["coal", "oil", "gas"]
    measures = [
        "existing_capacity_mw",
        "existing_co2e_tonnes_per_year",
        "existing_facility_count",
        "proposed_capacity_mw",
        "proposed_co2e_tonnes_per_year",
        "proposed_facility_count",
    ]
    for measure in measures:
        renewable_cols_to_sum = [
            "_".join([renewable, measure])
            for renewable in renewables
            if "_".join([renewable, measure]) in wide.columns
        ]
        fossil_cols_to_sum = [
            "_".join([fossil, measure])
            for fossil in fossils
            if "_".join([fossil, measure]) in wide.columns
        ]
        wide[f"renewable_and_battery_{measure}"] = wide.loc[
            :, renewable_cols_to_sum
        ].sum(axis=1)
        wide[f"fossil_{measure}"] = wide.loc[:, fossil_cols_to_sum].sum(axis=1)

    # infrastructure category totals
    measures = [
        "proposed_co2e_tonnes_per_year",
        "proposed_facility_count",
        "proposed_nox_tonnes_per_year",
        "proposed_pm2_5_tonnes_per_year",
    ]
    sectors = [
        "gas",
        "lng",
        "oil",
        "petrochemicals_and_plastics",
        "synthetic_fertilizers",
    ]
    for measure in measures:
        infra_cols_to_sum = [f"infra_{sector}_{measure}" for sector in sectors]
        wide[f"infra_total_{measure}"] = wide.loc[:, infra_cols_to_sum].sum(axis=1)

    wide.dropna(axis=1, how="all", inplace=True)
    cols_to_drop = [
        # A handful of hybrid facilities with co-located diesel generators.
        # They produce tiny amounts of CO2 but large amounts of confusion.
        "onshore_wind_existing_co2e_tonnes_per_year",
        "battery_storage_existing_co2e_tonnes_per_year",
    ]
    wide.drop(columns=cols_to_drop, inplace=True)

    col_order = [
        "state_id_fips",
        "county_id_fips",
        "state",
        "county",
        "has_ordinance",
        "state_permitting_type",
        "county_total_co2e_tonnes_per_year",
        "fossil_existing_capacity_mw",
        "fossil_existing_co2e_tonnes_per_year",
        "fossil_existing_facility_count",
        "fossil_proposed_capacity_mw",
        "fossil_proposed_co2e_tonnes_per_year",
        "fossil_proposed_facility_count",
        "renewable_and_battery_existing_capacity_mw",
        "renewable_and_battery_existing_co2e_tonnes_per_year",  # left in because of hybrid plants like solar thermal
        "renewable_and_battery_existing_facility_count",
        "renewable_and_battery_proposed_capacity_mw",
        "renewable_and_battery_proposed_facility_count",
        "infra_total_proposed_co2e_tonnes_per_year",
        "infra_total_proposed_facility_count",
        "infra_total_proposed_nox_tonnes_per_year",
        "infra_total_proposed_pm2_5_tonnes_per_year",
        "battery_storage_existing_capacity_mw",
        "battery_storage_existing_facility_count",
        "battery_storage_proposed_capacity_mw",
        "battery_storage_proposed_facility_count",
        "coal_existing_capacity_mw",
        "coal_existing_co2e_tonnes_per_year",
        "coal_existing_facility_count",
        "coal_proposed_capacity_mw",
        "coal_proposed_co2e_tonnes_per_year",
        "coal_proposed_facility_count",
        "gas_existing_capacity_mw",
        "gas_existing_co2e_tonnes_per_year",
        "gas_existing_facility_count",
        "gas_proposed_capacity_mw",
        "gas_proposed_co2e_tonnes_per_year",
        "gas_proposed_facility_count",
        "offshore_wind_existing_capacity_mw",
        "offshore_wind_existing_facility_count",
        "offshore_wind_proposed_capacity_mw",
        "offshore_wind_proposed_facility_count",
        "oil_existing_capacity_mw",
        "oil_existing_co2e_tonnes_per_year",
        "oil_existing_facility_count",
        "onshore_wind_existing_capacity_mw",
        "onshore_wind_existing_facility_count",
        "onshore_wind_proposed_capacity_mw",
        "onshore_wind_proposed_facility_count",
        "solar_existing_capacity_mw",
        "solar_existing_co2e_tonnes_per_year",
        "solar_existing_facility_count",
        "solar_proposed_capacity_mw",
        "solar_proposed_facility_count",
        "infra_gas_proposed_co2e_tonnes_per_year",
        "infra_gas_proposed_facility_count",
        "infra_gas_proposed_nox_tonnes_per_year",
        "infra_gas_proposed_pm2_5_tonnes_per_year",
        "infra_lng_proposed_co2e_tonnes_per_year",
        "infra_lng_proposed_facility_count",
        "infra_lng_proposed_nox_tonnes_per_year",
        "infra_lng_proposed_pm2_5_tonnes_per_year",
        "infra_oil_proposed_co2e_tonnes_per_year",
        "infra_oil_proposed_facility_count",
        "infra_oil_proposed_nox_tonnes_per_year",
        "infra_oil_proposed_pm2_5_tonnes_per_year",
        "infra_petrochemicals_and_plastics_proposed_co2e_tonnes_per_year",
        "infra_petrochemicals_and_plastics_proposed_facility_count",
        "infra_petrochemicals_and_plastics_proposed_nox_tonnes_per_year",
        "infra_petrochemicals_and_plastics_proposed_pm2_5_tonnes_per_year",
        "infra_synthetic_fertilizers_proposed_co2e_tonnes_per_year",
        "infra_synthetic_fertilizers_proposed_facility_count",
        "infra_synthetic_fertilizers_proposed_nox_tonnes_per_year",
        "infra_synthetic_fertilizers_proposed_pm2_5_tonnes_per_year",
        "total_tracts",
        "n_tracts_disadvantaged",
        "n_tracts_agriculture_loss_low_income",
        "n_tracts_building_loss_low_income",
        "n_tracts_population_loss_low_income",
        "n_tracts_diesel_particulates_low_income",
        "n_tracts_energy_burden_low_income",
        "n_tracts_pm2_5_low_income",
        "n_tracts_traffic_low_income",
        "n_tracts_lead_paint_houses_and_median_home_price_low_income",
        "n_tracts_housing_burden_low_income",
        "n_tracts_risk_management_plan_proximity_low_income",
        "n_tracts_superfund_proximity_low_income",
        "n_tracts_wastewater_low_income",
        "n_tracts_asthma_low_income",
        "n_tracts_heart_disease_low_income",
        "n_tracts_diabetes_low_income",
        "n_tracts_local_to_area_income_ratio_and_low_high_school",
        "n_tracts_linguistic_isolation_and_low_high_school",
        "n_tracts_below_poverty_and_low_high_school",
        "n_tracts_unemployment_and_low_high_school",
        "n_tracts_hazardous_waste_proximity_low_income",
        "n_tracts_life_expectancy_low_income",
        "ordinance",
        "ordinance_earliest_year_mentioned",
        "ordinance_jurisdiction_name",
        "ordinance_jurisdiction_type",
        "state_permitting_text",
    ]
    return wide.loc[:, col_order].copy()


def create_long_format(
    postgres_engine: Optional[sa.engine.Engine] = None,
    pudl_engine: Optional[sa.engine.Engine] = None,
) -> pd.DataFrame:
    """Create the long format county datamart dataframe."""
    all_counties = _get_county_fips_df(postgres_engine)
    all_states = _get_state_fips_df(postgres_engine)
    county_properties = _get_county_properties(postgres_engine=postgres_engine)
    iso = _iso_projects_counties(postgres_engine)
    infra = _fossil_infrastructure_counties(postgres_engine)
    existing = _existing_plants_counties(
        pudl_engine=pudl_engine,
        postgres_engine=postgres_engine,
        state_fips_table=all_states,
        county_fips_table=all_counties,
    )

    # join it all
    out = pd.concat([iso, existing, infra], axis=0, ignore_index=True)
    out = out.merge(county_properties, on="county_id_fips", how="left")
    out = _add_derived_columns(out)
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
        "total_tracts",
        "n_tracts_disadvantaged",
        "n_tracts_agriculture_loss_low_income",
        "n_tracts_building_loss_low_income",
        "n_tracts_population_loss_low_income",
        "n_tracts_diesel_particulates_low_income",
        "n_tracts_energy_burden_low_income",
        "n_tracts_pm2_5_low_income",
        "n_tracts_traffic_low_income",
        "n_tracts_lead_paint_houses_and_median_home_price_low_income",
        "n_tracts_housing_burden_low_income",
        "n_tracts_risk_management_plan_proximity_low_income",
        "n_tracts_superfund_proximity_low_income",
        "n_tracts_wastewater_low_income",
        "n_tracts_asthma_low_income",
        "n_tracts_heart_disease_low_income",
        "n_tracts_diabetes_low_income",
        "n_tracts_local_to_area_income_ratio_and_low_high_school",
        "n_tracts_linguistic_isolation_and_low_high_school",
        "n_tracts_below_poverty_and_low_high_school",
        "n_tracts_unemployment_and_low_high_school",
        "n_tracts_hazardous_waste_proximity_low_income",
        "n_tracts_life_expectancy_low_income",
    ]
    return out.loc[:, col_order]


def _get_county_properties(
    postgres_engine: sa.engine.Engine,
    include_state_policies=False,
    rename_dict: Optional[Dict[str, str]] = None,
):
    if rename_dict is None:
        rename_dict = {
            "geocoded_locality_name": "ordinance_jurisdiction_name",
            "geocoded_locality_type": "ordinance_jurisdiction_type",
            "earliest_year_mentioned": "ordinance_earliest_year_mentioned",
            "description": "state_permitting_text",
            "permitting_type": "state_permitting_type",
            "state_name": "state",
            "county_name": "county",
        }
    ncsl = _get_ncsl_wind_permitting_df(postgres_engine)
    all_counties = _get_county_fips_df(postgres_engine)
    all_states = _get_state_fips_df(postgres_engine)
    env_justice = _get_env_justice_df(postgres_engine)
    # model local opposition
    aggregator = CountyOpposition(
        engine=postgres_engine, county_fips_df=all_counties, state_fips_df=all_states
    )
    combined_opp = aggregator.agg_to_counties(
        include_state_policies=include_state_policies
    )

    county_properties = all_counties[["county_name", "county_id_fips"]].merge(
        combined_opp, on="county_id_fips", how="left"
    )
    county_properties["state_id_fips"] = county_properties["county_id_fips"].str[:2]
    county_properties = county_properties.merge(
        ncsl, on="state_id_fips", how="left", validate="m:1"
    )
    county_properties = county_properties.merge(
        all_states[["state_name", "state_id_fips"]],
        on="state_id_fips",
        how="left",
        validate="m:1",
    )
    county_properties = county_properties.merge(
        env_justice, on="county_id_fips", how="left", validate="1:1"
    )

    county_properties = county_properties.rename(columns=rename_dict)
    return county_properties


def _join_all_counties_to_wide_format(
    wide_format: pd.DataFrame, county_properties: pd.DataFrame
):
    county_properties = _add_derived_columns(county_properties)
    wide_format_column_order = wide_format.columns.copy()

    county_columns = [
        "state_id_fips",
        "state",
        "county",
        "has_ordinance",
        "state_permitting_type",
        "ordinance",
        "ordinance_earliest_year_mentioned",
        "ordinance_jurisdiction_name",
        "ordinance_jurisdiction_type",
        "state_permitting_text",
        "total_tracts",
        "n_tracts_disadvantaged",
        "n_tracts_agriculture_loss_low_income",
        "n_tracts_building_loss_low_income",
        "n_tracts_population_loss_low_income",
        "n_tracts_diesel_particulates_low_income",
        "n_tracts_energy_burden_low_income",
        "n_tracts_pm2_5_low_income",
        "n_tracts_traffic_low_income",
        "n_tracts_lead_paint_houses_and_median_home_price_low_income",
        "n_tracts_housing_burden_low_income",
        "n_tracts_risk_management_plan_proximity_low_income",
        "n_tracts_superfund_proximity_low_income",
        "n_tracts_wastewater_low_income",
        "n_tracts_asthma_low_income",
        "n_tracts_heart_disease_low_income",
        "n_tracts_diabetes_low_income",
        "n_tracts_local_to_area_income_ratio_and_low_high_school",
        "n_tracts_linguistic_isolation_and_low_high_school",
        "n_tracts_below_poverty_and_low_high_school",
        "n_tracts_unemployment_and_low_high_school",
        "n_tracts_hazardous_waste_proximity_low_income",
        "n_tracts_life_expectancy_low_income",
    ]
    wide_format_subset = wide_format.drop(columns=county_columns)
    county_properties = county_properties.merge(
        wide_format_subset,
        on="county_id_fips",
        how="left",
    )
    return county_properties[wide_format_column_order]


def create_data_mart(
    engine: Optional[sa.engine.Engine] = None,
    pudl_engine: Optional[sa.engine.Engine] = None,
) -> Dict[str, pd.DataFrame]:
    """Create county data marts.

    Args:
        engine (Optional[sa.engine.Engine], optional): postgres engine. Defaults to None.
        pudl_engine (Optional[sa.engine.Engine], optional): sqlite engine. Defaults to None.

    Returns:
        Dict[str, pd.DataFrame]: county tables in both wide and long format
    """
    postgres_engine = engine
    if postgres_engine is None:
        postgres_engine = get_sql_engine()
    if pudl_engine is None:
        pudl_engine = get_pudl_engine()

    long_format = create_long_format(
        postgres_engine=postgres_engine, pudl_engine=pudl_engine
    )
    wide_format = _convert_long_to_wide(long_format)
    # client requested joining all counties onto wide format table, even if all values are NULL
    county_properties = _get_county_properties(postgres_engine)
    wide_format = _join_all_counties_to_wide_format(wide_format, county_properties)

    out = {
        "counties_long_format": long_format,
        "counties_wide_format": wide_format,
    }
    return out
