"""Module to create a county-level table for DBCP to use in spreadsheet tools."""
from io import StringIO
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

JUSTICE40_AGGREGATES = pd.read_csv(
    # This variable exists because of Postgres character limits on column names.
    # These column names are referenced 5 times in this  module and I don't want to
    # rely on copy/paste if they change. Changing them is especially awkward because
    # PostgreSQL has a 64 character limit on column names, so some of the given names
    # in transform.justice40.py AND in subsequent queries get truncated, meaning the
    # given names and actual names are different.
    # The source of these names is the query in _get_env_justice_df + truncation.
    # I added the categories here (used in _create_dbcp_ej_index) out of convenience.
    StringIO(
        """name,category
total_tracts,
justice40_dbcp_index,
n_distinct_qualifying_tracts,
n_tracts_agriculture_loss_low_income_not_students,climate
n_tracts_asthma_low_income_not_students,health
n_tracts_below_poverty_and_low_high_school,workforce
n_tracts_below_poverty_line_less_than_high_school_islands,workforce
n_tracts_building_loss_low_income_not_students,climate
n_tracts_diabetes_low_income_not_students,health
n_tracts_diesel_particulates_low_income_not_students,transit
n_tracts_energy_burden_low_income_not_students,energy
n_tracts_hazardous_waste_proximity_low_income_not_students,pollution
n_tracts_heart_disease_low_income_not_students,health
n_tracts_housing_burden_low_income_not_students,housing
n_tracts_lead_paint_and_median_home_price_low_income_not_studen,housing
n_tracts_life_expectancy_low_income_not_students,health
n_tracts_linguistic_isolation_and_low_high_school,workforce
n_tracts_local_to_area_income_ratio_and_low_high_school,workforce
n_tracts_local_to_area_income_ratio_less_than_high_school_islan,workforce
n_tracts_pm2_5_low_income_not_students,energy
n_tracts_population_loss_low_income_not_students,climate
n_tracts_risk_management_plan_proximity_low_income_not_students,pollution
n_tracts_superfund_proximity_low_income_not_students,pollution
n_tracts_traffic_low_income_not_students,transit
n_tracts_unemployment_and_low_high_school,workforce
n_tracts_unemployment_less_than_high_school_islands,workforce
n_tracts_wastewater_low_income_not_students,water
"""
    )
)


def _create_dbcp_ej_index(j40_df: pd.DataFrame) -> pd.Series:
    """Derive an environmental justice score based on Justice40 data.

    This algorithm was specified by the client as follows:
    1. By counties: If any of the indicators (within a category) has >1 (meaning at least 1 tract is affected), then it's a YES.
    2. Each indicator category has a unique weight, so each YES is counted by the weight assigned to that category
    3. For each county, sum all of the YES indicator categories with its appropriate weight.
    4. Final number sums all indicator catergories and gives you the Justice 40 DBCP Index.
    """
    category_weights = {  # specified by client
        "climate": 1.0,
        "energy": 1.0,
        "transit": 1.0,
        "pollution": 0.75,
        "water": 0.75,
        "housing": 0.5,
        "health": 0.5,
        "workforce": 0.5,
    }
    mismatched_categories = set(
        JUSTICE40_AGGREGATES["category"].dropna().unique()
    ).symmetric_difference(set(category_weights.keys()))
    err_msg = (
        f"category_weights.keys() should match categories in JUSTICE40_AGGREGATES. "
        f"The following do not match: {mismatched_categories}"
    )
    assert len(mismatched_categories) == 0, err_msg

    score_components = []
    for category, weight in category_weights.items():
        column_subset = JUSTICE40_AGGREGATES.loc[
            JUSTICE40_AGGREGATES["category"] == category, "name"
        ].to_list()
        weighted_indicator = (
            j40_df[column_subset].sum(axis=1).gt(0).astype(float) * weight
        )
        score_components.append(weighted_indicator)
    score = pd.concat(score_components, axis=1).sum(axis=1)
    score.name = "justice40_dbcp_index"
    return score


def _get_env_justice_df(engine: sa.engine.Engine) -> pd.DataFrame:
    """Create county-level aggregates of Justice40 tracts."""
    query = """
    SELECT
        SUBSTRING("tract_id_fips", 1, 5) as county_id_fips,
        COUNT("tract_id_fips") as total_tracts,
        SUM("is_disadvantaged"::INTEGER) as n_distinct_qualifying_tracts,
        SUM("expected_agriculture_loss_and_low_income_and_not_students"::INTEGER) as n_tracts_agriculture_loss_low_income_not_students,
        SUM("expected_building_loss_and_low_income_and_not_students"::INTEGER) as n_tracts_building_loss_low_income_not_students,
        SUM("expected_population_loss_and_low_income_and_not_students"::INTEGER) as n_tracts_population_loss_low_income_not_students,
        SUM("diesel_particulates_and_low_income_and_not_students"::INTEGER) as n_tracts_diesel_particulates_low_income_not_students,
        SUM("energy_burden_and_low_income_and_not_students"::INTEGER) as n_tracts_energy_burden_low_income_not_students,
        SUM("pm2_5_and_low_income_and_not_students"::INTEGER) as n_tracts_pm2_5_low_income_not_students,
        SUM("traffic_and_low_income_and_not_students"::INTEGER) as n_tracts_traffic_low_income_not_students,
        SUM("lead_paint_and_median_home_price_and_low_income_and_not_student"::INTEGER) as n_tracts_lead_paint_and_median_home_price_low_income_not_students,
        SUM("housing_burden_and_low_income_and_not_students"::INTEGER) as n_tracts_housing_burden_low_income_not_students,
        SUM("risk_management_plan_proximity_and_low_income_and_not_students"::INTEGER) as n_tracts_risk_management_plan_proximity_low_income_not_students,
        SUM("superfund_proximity_and_low_income_and_not_students"::INTEGER) as n_tracts_superfund_proximity_low_income_not_students,
        SUM("wastewater_and_low_income_and_not_students"::INTEGER) as n_tracts_wastewater_low_income_not_students,
        SUM("asthma_and_low_income_and_not_students"::INTEGER) as n_tracts_asthma_low_income_not_students,
        SUM("heart_disease_and_low_income_and_not_students"::INTEGER) as n_tracts_heart_disease_low_income_not_students,
        SUM("diabetes_and_low_income_and_not_students"::INTEGER) as n_tracts_diabetes_low_income_not_students,
        SUM("local_to_area_income_ratio_and_less_than_high_school_and_not_st"::INTEGER) as n_tracts_local_to_area_income_ratio_and_low_high_school,
        SUM("linguistic_isolation_and_less_than_high_school_and_not_students"::INTEGER) as n_tracts_linguistic_isolation_and_low_high_school,
        SUM("below_poverty_line_and_less_than_high_school_and_not_students"::INTEGER) as n_tracts_below_poverty_and_low_high_school,
        SUM("unemployment_and_less_than_high_school_and_not_students"::INTEGER) as n_tracts_unemployment_and_low_high_school,
        SUM("hazardous_waste_proximity_and_low_income_and_not_students"::INTEGER) as n_tracts_hazardous_waste_proximity_low_income_not_students,
        SUM("unemployment_and_less_than_high_school_islands"::INTEGER) as n_tracts_unemployment_less_than_high_school_islands,
        SUM("local_to_area_income_ratio_and_less_than_high_school_islands"::INTEGER) as n_tracts_local_to_area_income_ratio_less_than_high_school_islands,
        SUM("below_poverty_line_and_less_than_high_school_islands"::INTEGER) as n_tracts_below_poverty_line_less_than_high_school_islands,
        SUM("life_expectancy_and_low_income_and_not_students"::INTEGER) as n_tracts_life_expectancy_low_income_not_students
    FROM "data_warehouse"."justice40_tracts"
    GROUP BY 1;
    """
    df = pd.read_sql(query, engine)
    df["justice40_dbcp_index"] = _create_dbcp_ej_index(df)
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
) -> pd.Series:
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
            "co2e_tonnes_per_year": "sum",  # type: ignore
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


def _get_curated_offshore_wind_df(engine: sa.engine.Engine) -> pd.DataFrame:
    """Offshore wind with capacity split by cable landing location then aggregated to county.

    Output matches structure of _iso_projects_counties() so we can easily replace
    the less certain ISO offshore projects with these.
    """
    query = """
    WITH
    proj_landings AS (
        SELECT
            proj."project_id",
            cable.location_id,
            locs.county_id_fips,
            proj."capacity_mw",
            COUNT(*) OVER(PARTITION BY proj."project_id") AS n_landings
        FROM "data_warehouse"."offshore_wind_projects" as proj
        INNER JOIN data_warehouse.offshore_wind_cable_landing_association as cable
        USING(project_id)
        INNER JOIN data_warehouse.offshore_wind_locations as locs
        USING(location_id)
    )
    SELECT
        county_id_fips,
        'Offshore Wind' as resource_or_sector,
        count( distinct (project_id)) as facility_count,
        NULL as co2e_tonnes_per_year,
        SUM(capacity_mw / n_landings) as capacity_mw,
        'power plant' as facility_type,
        'proposed' as status
    FROM proj_landings
    GROUP BY 1
    ;
    """
    df = pd.read_sql(query, engine)
    return df


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


def _add_derived_columns(mart: pd.DataFrame) -> pd.DataFrame:
    out = mart.copy()
    out["has_ordinance"] = out["ordinance"].notna()
    ban_cols = [
        "has_ordinance",
        "has_solar_ban_nrel",
        "has_wind_ban_nrel",
    ]
    out["has_ban"] = out[ban_cols].fillna(False).any(axis=1)

    return out


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
        "has_solar_ban_nrel",
        "has_wind_ban_nrel",
        "has_de_facto_ban_nrel",
        "has_ban",
        "unprotected_land_area_km2",
        "federal_fraction_unprotected_land",
        "county_land_area_km2",
        "ec_qualifies_via_brownfield",
        "ec_qualifies_via_coal_closures",
        "ec_qualifies_via_employment",
        "ec_qualifies",
    ] + JUSTICE40_AGGREGATES["name"].to_list()
    wide = long.pivot(index=idx_cols, columns=col_cols, values=val_cols)

    wide.columns = wide.columns.rename(
        {None: "measures"}
    )  # value columns: capacity, count, etc
    wide.columns = wide.columns.reorder_levels(col_cols + ["measures"])
    wide.columns = [
        "_".join(col).strip("_") for col in wide.columns
    ]  # flatten multiindex

    # this is not equivalent to left joining _get_county_properties() because
    # long does not have data for every county. _get_county_properties() does.
    # I think we can replace this with _join_all_counties_to_wide_format but
    # don't want to do that refactor right this second.
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
        "has_ban",
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
        "has_ordinance",
        "ordinance",
        "ordinance_earliest_year_mentioned",
        "ordinance_jurisdiction_name",
        "ordinance_jurisdiction_type",
        "state_permitting_text",
        "has_solar_ban_nrel",
        "has_wind_ban_nrel",
        "has_de_facto_ban_nrel",
        "unprotected_land_area_km2",
        "federal_fraction_unprotected_land",
        "county_land_area_km2",
        "ec_qualifies_via_brownfield",
        "ec_qualifies_via_coal_closures",
        "ec_qualifies_via_employment",
        "ec_qualifies",
    ] + JUSTICE40_AGGREGATES["name"].to_list()
    return wide.loc[:, col_order].copy()


def create_long_format(
    postgres_engine: sa.engine.Engine,
    pudl_engine: sa.engine.Engine,
) -> pd.DataFrame:
    """Create the long format county datamart dataframe."""
    all_counties = _get_county_fips_df(postgres_engine)
    all_states = _get_state_fips_df(postgres_engine)
    county_properties = _get_county_properties(postgres_engine=postgres_engine)
    iso = _iso_projects_counties(postgres_engine)
    curated_offshore = _get_curated_offshore_wind_df(postgres_engine)
    infra = _fossil_infrastructure_counties(postgres_engine)
    existing = _existing_plants_counties(
        pudl_engine=pudl_engine,
        postgres_engine=postgres_engine,
        state_fips_table=all_states,
        county_fips_table=all_counties,
    )

    # replace ISO Offshore Wind with curated data
    iso = iso.loc[iso["resource_or_sector"] != "Offshore Wind", :]
    iso = pd.concat([iso, curated_offshore], axis=0, ignore_index=True)

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
        "has_ban",
        "ordinance_jurisdiction_name",
        "ordinance_jurisdiction_type",
        "has_ordinance",
        "ordinance",
        "ordinance_earliest_year_mentioned",
        "has_solar_ban_nrel",
        "has_wind_ban_nrel",
        "has_de_facto_ban_nrel",
        "state_permitting_type",
        "state_permitting_text",
        "unprotected_land_area_km2",
        "federal_fraction_unprotected_land",
        "county_land_area_km2",
        "ec_qualifies_via_brownfield",
        "ec_qualifies_via_coal_closures",
        "ec_qualifies_via_employment",
        "ec_qualifies",
    ] + JUSTICE40_AGGREGATES["name"].to_list()
    return out.loc[:, col_order]


def _get_offshore_wind_extra_cols(engine: sa.engine.Engine) -> pd.DataFrame:
    query = """
    WITH
    proj_ports AS (
        SELECT
            proj."project_id",
            port.location_id,
            locs.county_id_fips,
            proj."capacity_mw"
        FROM "data_warehouse"."offshore_wind_projects" as proj
        INNER JOIN data_warehouse.offshore_wind_port_association as port
        USING(project_id)
        INNER JOIN data_warehouse.offshore_wind_locations as locs
        USING(location_id)
    ),
    -- select * from proj_ports
    -- order by project_id, location_id
    port_aggs AS (
        SELECT
            county_id_fips,
            SUM(capacity_mw) as offshore_wind_capacity_mw_via_ports
        FROM proj_ports
        GROUP BY 1
        order by 1
    ),
    interest AS (
        SELECT
            county_id_fips,
            why_of_interest as offshore_wind_interest_type
        FROM data_warehouse.offshore_wind_locations as locs
        GROUP BY 1,2
        ORDER BY 1
    )
    SELECT
        county_id_fips,
        offshore_wind_capacity_mw_via_ports,
        offshore_wind_interest_type
    FROM interest
    LEFT JOIN port_aggs
    USING(county_id_fips)
    where county_id_fips is not NULL;
    """
    df = pd.read_sql(query, engine)
    return df


def _get_federal_land_fraction(postgres_engine: sa.engine.Engine):
    query = """
    select
        county_id_fips,
        gap_status,
        manager_type,
        intersection_area_padus_km2,
        county_area_coast_clipped_km2
    from data_warehouse.protected_area_by_county as pa
    """
    pad = pd.read_sql(query, postgres_engine)
    # county_area_coast_clipped is consistent with clipped PAD-US but
    # the county_fips.land_area_km2 is more accurate and preferred for
    # downstream analysis.
    # I use the consistent value to calculate ratio, then pair that ratio
    #  with the accurate land area in the data mart
    county_areas = pad.groupby("county_id_fips")[
        "county_area_coast_clipped_km2"
    ].first()
    is_developable = pad["gap_status"].str.match("^[34]")
    is_federally_managed = pad["manager_type"] == "Federal"

    federal_developable = (
        pad.loc[is_developable & is_federally_managed, :]
        .groupby("county_id_fips")["intersection_area_padus_km2"]
        .sum()
        .rename("fed_dev")
    )
    un_developable = (
        pad.loc[~is_developable, :]
        .groupby("county_id_fips")["intersection_area_padus_km2"]
        .sum()
        .rename("protected")
    )
    areas = pd.concat(
        [county_areas, federal_developable, un_developable], axis=1, join="outer"
    )
    areas.loc[:, ["fed_dev", "protected"]].fillna(0, inplace=True)
    areas["unprotected_land_area_km2"] = (
        areas["county_area_coast_clipped_km2"] - areas["protected"]
    )
    areas["federal_fraction_unprotected_land"] = (
        areas["fed_dev"] / areas["unprotected_land_area_km2"]
    )
    out_cols = [
        "unprotected_land_area_km2",
        "federal_fraction_unprotected_land",
    ]
    correlated_rounding_errors = areas["federal_fraction_unprotected_land"].gt(1)
    assert (
        correlated_rounding_errors.sum() == 1
    ), f"Expected 1 bad rounding error, got {correlated_rounding_errors.sum()}"
    areas.loc[
        correlated_rounding_errors, "federal_fraction_unprotected_land"
    ] = 1.0  # manually clip

    return areas.loc[:, out_cols].copy()


def _get_energy_community_qualification(postgres_engine: sa.engine.Engine):
    brownfield_threshold_acres = 7000  # ~0.1-0.2 MW solar potential per acre
    coal_area_threshold_km2 = 7000 / 247.105  # acres to km2

    query = [  # nosec - have to use this dumb list structure to put the "nosec" comment inline
        f"""
    WITH
    ec as (
        SELECT
            ec.county_id_fips,
            brownfield_acreage_mean_fill > {brownfield_threshold_acres} as ec_qualifies_via_brownfield,
            (coal_qualifying_area_fraction * fips.land_area_km2) > {coal_area_threshold_km2} as ec_qualifies_via_coal_closures,
            qualifies_by_employment_criteria as ec_qualifies_via_employment
        FROM data_warehouse.energy_communities_by_county AS ec
        LEFT JOIN data_warehouse.county_fips AS fips
        USING (county_id_fips)
    )
    SELECT
        *,
        (ec_qualifies_via_brownfield OR
        ec_qualifies_via_coal_closures OR
        ec_qualifies_via_employment) as ec_qualifies
    FROM ec
    """
    ][
        0
    ]
    ec = pd.read_sql(query, postgres_engine)

    return ec


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
            "land_area_km2": "county_land_area_km2",
        }
    ncsl = _get_ncsl_wind_permitting_df(postgres_engine)
    all_counties = _get_county_fips_df(postgres_engine)
    all_states = _get_state_fips_df(postgres_engine)
    env_justice = _get_env_justice_df(postgres_engine)
    fed_lands = _get_federal_land_fraction(postgres_engine)
    ec_counties = _get_energy_community_qualification(postgres_engine)

    # model local opposition
    aggregator = CountyOpposition(
        engine=postgres_engine, county_fips_df=all_counties, state_fips_df=all_states
    )
    combined_opp = aggregator.agg_to_counties(
        include_state_policies=include_state_policies,
        include_nrel_bans=True,
    )

    county_properties = all_counties[
        ["county_name", "county_id_fips", "state_id_fips", "land_area_km2"]
    ].merge(combined_opp, on="county_id_fips", how="left")
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
    # NOTE: the federal lands dataset uses 2022 FIPS codes.
    # Some (02063) are not present in the rest of the datasets.
    # Non-matching FIPS are currently dropped.
    county_properties = county_properties.merge(
        fed_lands, on="county_id_fips", how="left", validate="1:1"
    )

    # NOTE: the ec dataset uses 2010 FIPS codes.
    # Some (02261) are not present in the rest of the datasets.
    # Non-matching FIPS are currently dropped.
    county_properties = county_properties.merge(
        ec_counties, on="county_id_fips", how="left", validate="1:1"
    )

    county_properties = county_properties.rename(columns=rename_dict)
    return county_properties


def _join_all_counties_to_wide_format(
    wide_format: pd.DataFrame, county_properties: pd.DataFrame
):
    # this exists to create a row for every county, even if it is all nulls.
    # The long format data does not include rows for all counties, so
    # _convert_long_to_wide does not produce a row for each county.
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
        "has_solar_ban_nrel",
        "has_wind_ban_nrel",
        "has_de_facto_ban_nrel",
        "has_ban",
        "unprotected_land_area_km2",
        "federal_fraction_unprotected_land",
        "county_land_area_km2",
        "ec_qualifies_via_brownfield",
        "ec_qualifies_via_coal_closures",
        "ec_qualifies_via_employment",
        "ec_qualifies",
    ] + JUSTICE40_AGGREGATES["name"].to_list()
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
    # client requested two additional columns relating to offshore wind
    offshore_bits = _get_offshore_wind_extra_cols(postgres_engine)
    wide_format = wide_format.merge(
        offshore_bits, on="county_id_fips", how="left", copy=False
    )

    out = {
        "counties_long_format": long_format,
        "counties_wide_format": wide_format,
    }
    return out
