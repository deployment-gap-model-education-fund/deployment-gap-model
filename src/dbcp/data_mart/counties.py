"""Module to create a county-level table for DBCP to use in spreadsheet tools.

This module creates two tables: county_wide_format and county_long_format.
The long format primary key is (county, resource), while the wide format
key is simply county.

The long format table is used primarily by tableau, while the wide format
is most useful for spreadsheet tools. The wide format table is easier to
understand for non-data people because 1 row = 1 county, and gets more
use by the clients themselves. The long format table, via tableau, gets
more use by the client's clients.

The two tables began with the same data, but over time the wide format has
accumulated ad hoc requests from the client, so they have diverged somewhat.

For the sake of maintainability, the wide format table is generated from the
long format table as much as possible. But some columns must be calculated
separately, such as non-commutative aggregations over nested groupings, eg
`count(distinct my_column)) group by county, resource` cannot have another
`count(distinct my_column) group by county` on top of it.
"""

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
    get_query,
)
from dbcp.data_mart.projects import create_fyi_long_format as create_fyi_data_mart
from dbcp.helpers import get_sql_engine

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
n_tracts_agriculture_loss_low_income,climate
n_tracts_asthma_low_income,health
n_tracts_below_poverty_and_low_high_school,workforce
n_tracts_below_poverty_line_less_than_high_school_islands,workforce
n_tracts_building_loss_low_income,climate
n_tracts_diabetes_low_income,health
n_tracts_diesel_particulates_low_income,transit
n_tracts_energy_burden_low_income,energy
n_tracts_hazardous_waste_proximity_low_income,pollution
n_tracts_heart_disease_low_income,health
n_tracts_housing_burden_low_income,housing
n_tracts_lead_paint_and_median_home_price_low_income,housing
n_tracts_life_expectancy_low_income,health
n_tracts_linguistic_isolation_and_low_high_school,workforce
n_tracts_local_to_area_income_ratio_and_low_high_school,workforce
n_tracts_local_to_area_income_ratio_less_than_high_school_islan,workforce
n_tracts_pm2_5_low_income,energy
n_tracts_population_loss_low_income,climate
n_tracts_superfund_proximity_low_income,pollution
n_tracts_traffic_low_income,transit
n_tracts_unemployment_and_low_high_school,workforce
n_tracts_unemployment_less_than_high_school_islands,workforce
n_tracts_wastewater_low_income,water
"""
    )
)


RENEWABLE_TYPES = ("Solar", "Offshore Wind", "Onshore Wind", "Battery Storage")
FOSSIL_TYPES = ("Coal", "Oil", "Gas")


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
    df = pd.read_sql_table("justice40_tracts", engine, schema="data_warehouse")
    df["county_id_fips"] = df["tract_id_fips"].str.slice(0, 5)
    df = df.groupby("county_id_fips").agg(
        total_tracts=("tract_id_fips", "count"),
        n_distinct_qualifying_tracts=("is_disadvantaged", "sum"),
        n_tracts_agriculture_loss_low_income=(
            "expected_agriculture_loss_rate_is_low_income",
            "sum",
        ),
        n_tracts_building_loss_low_income=(
            "expected_building_loss_rate_is_low_income",
            "sum",
        ),
        n_tracts_population_loss_low_income=(
            "expected_population_loss_rate_is_low_income",
            "sum",
        ),
        n_tracts_diesel_particulates_low_income=(
            "diesel_particulates_is_low_income",
            "sum",
        ),
        n_tracts_energy_burden_low_income=("energy_burden_is_low_income", "sum"),
        n_tracts_pm2_5_low_income=("pm2_5_is_low_income", "sum"),
        n_tracts_traffic_low_income=("traffic_proximity_is_low_income", "sum"),
        n_tracts_lead_paint_and_median_home_price_low_income=(
            "lead_paint_and_median_house_value_is_low_income",
            "sum",
        ),
        n_tracts_housing_burden_low_income=("housing_burden_is_low_income", "sum"),
        n_tracts_superfund_proximity_low_income=(
            "proximity_to_superfund_sites_is_low_income",
            "sum",
        ),
        n_tracts_wastewater_low_income=("wastewater_discharge_is_low_income", "sum"),
        n_tracts_asthma_low_income=("asthma_is_low_income", "sum"),
        n_tracts_heart_disease_low_income=("heart_disease_is_low_income", "sum"),
        n_tracts_diabetes_low_income=("diabetes_is_low_income", "sum"),
        n_tracts_local_to_area_income_ratio_and_low_high_school=(
            "low_median_household_income_and_low_hs_attainment",
            "sum",
        ),
        n_tracts_linguistic_isolation_and_low_high_school=(
            "households_in_linguistic_isolation_and_low_hs_attainment",
            "sum",
        ),
        n_tracts_below_poverty_and_low_high_school=(
            "households_below_federal_poverty_level_low_hs_attainment",
            "sum",
        ),
        n_tracts_unemployment_and_low_high_school=(
            "unemployment_and_low_hs_attainment",
            "sum",
        ),
        n_tracts_hazardous_waste_proximity_low_income=(
            "proximity_to_hazardous_waste_facilities_is_low_income",
            "sum",
        ),
        n_tracts_unemployment_less_than_high_school_islands=(
            "unemployment_and_low_hs_edu_islands",
            "sum",
        ),
        n_tracts_local_to_area_income_ratio_less_than_high_school_islan=(
            "low_median_household_income_and_low_hs_edu_islands",
            "sum",
        ),
        n_tracts_below_poverty_line_less_than_high_school_islands=(
            "households_below_federal_poverty_level_low_hs_edu_islands",
            "sum",
        ),
        n_tracts_life_expectancy_low_income=(
            "low_life_expectancy_is_low_income",
            "sum",
        ),
    )
    df["justice40_dbcp_index"] = _create_dbcp_ej_index(df)
    return df


def _get_existing_plant_attributes(engine: sa.engine.Engine) -> pd.DataFrame:
    # get plant_id, fuel_type, capacity_mw

    # The query here relies on the fact that each generator has only one fuel_type_pudl.
    # I confirmed that this is true because the following query returns 1:
    # WITH
    # gen_fuels as (
    #     SELECT
    #         plant_id_eia,
    #         generator_id,
    #         count(fuel_type_code_pudl) as fuel_type_count
    #     FROM "data_warehouse"."pudl_generators"
    #     GROUP BY 1, 2
    # )
    # SELECT max(fuel_type_count) as max_fuel_type_count
    # FROM gen_fuels

    query = get_query("get_existing_plant_attributes.sql")
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


def _get_existing_fossil_plant_co2e_estimates() -> pd.Series:
    gen_fuel_923 = _get_existing_plant_fuel_data()
    plant_co2e = _estimate_existing_co2e(gen_fuel_923)
    return plant_co2e


def _get_existing_plant_locations(
    postgres_engine: sa.engine.Engine,
    state_fips_table: Optional[pd.DataFrame] = None,
    county_fips_table: Optional[pd.DataFrame] = None,
):
    if state_fips_table is None:
        state_fips_table = _get_state_fips_df(postgres_engine)
    if county_fips_table is None:
        county_fips_table = _get_county_fips_df(postgres_engine)
    plant_locations = _get_plant_location_data()
    plant_locations = _transfrom_plant_location_data(
        plant_locations, state_table=state_fips_table, county_table=county_fips_table
    )
    return plant_locations


def _get_existing_plants(
    postgres_engine: sa.engine.Engine,
    state_fips_table: Optional[pd.DataFrame] = None,
    county_fips_table: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    plants = _get_existing_plant_attributes(engine=postgres_engine)
    co2e = _get_existing_fossil_plant_co2e_estimates()
    locations = _get_existing_plant_locations(
        postgres_engine=postgres_engine,
        state_fips_table=state_fips_table,
        county_fips_table=county_fips_table,
    )
    plants = plants.merge(co2e, how="left", on="plant_id_eia", copy=False)
    plants = plants.merge(locations, how="left", on="plant_id_eia", copy=False)
    return plants


def _existing_plants_counties(
    postgres_engine: sa.engine.Engine,
    state_fips_table: Optional[pd.DataFrame] = None,
    county_fips_table: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Create existing plant county-plant aggs for the long-format county table."""
    plants = _get_existing_plants(
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
    aggs.loc[:, "co2e_tonnes_per_year"] = aggs.loc[:, "co2e_tonnes_per_year"].replace(
        0, np.nan
    )  # sums of 0 are simply unmodeled
    aggs["facility_type"] = "power plant"
    aggs["status"] = "existing"
    aggs = aggs.reset_index()
    aggs = aggs.rename(
        columns={
            "plant_id_eia": "facility_count",
            "resource": "resource_or_sector",
        },
    )
    return aggs


def _fossil_infrastructure_counties(engine: sa.engine.Engine) -> pd.DataFrame:
    # Avoid db dependency order by recreating the df.
    # Could also make an orchestration script.
    infra = create_fossil_infra_data_mart(engine)

    # equivalent SQL query that I translated to pandas to avoid dependency
    # on the data_mart schema (which doesn't yet exist when this function runs)
    # SELECT
    #     county_id_fips,
    #     industry_sector as resource_or_sector,
    #     count(project_id) as facility_count,
    #     sum(co2e_tonnes_per_year) as co2e_tonnes_per_year,
    #     sum(pm2_5_tonnes_per_year) as pm2_5_tonnes_per_year,
    #     sum(nox_tonnes_per_year) as nox_tonnes_per_year,
    #     'fossil infrastructure' as facility_type,
    #     'proposed' as status
    # from data_mart.fossil_infrastructure_projects
    # group by 1, 2

    infra.loc[:, "industry_sector"] = infra.loc[:, "industry_sector"].replace(
        "Liquefied Natural Gas (LNG)", "Liquefied Natural Gas"
    )  # Use shorthand code to shorten column names later on

    grp = infra.groupby(["county_id_fips", "industry_sector"])
    aggs = grp.agg(
        {
            "co2e_tonnes_per_year": "sum",
            "pm2_5_tonnes_per_year": "sum",
            "nox_tonnes_per_year": "sum",
            "project_id": "count",
        }
    )
    aggs.loc[:, "co2e_tonnes_per_year"] = aggs.loc[:, "co2e_tonnes_per_year"].replace(
        0, np.nan
    )  # sums of 0 are simply unmodeled

    aggs["facility_type"] = "fossil infrastructure"
    aggs["status"] = "proposed"
    aggs = aggs.reset_index()
    aggs.rename(
        columns={
            "project_id": "facility_count",
            "industry_sector": "resource_or_sector",
        },
        inplace=True,
    )
    return aggs


def _fyi_projects_counties(engine: sa.engine.Engine) -> pd.DataFrame:
    # Avoid db dependency order by recreating the df.
    # Could also make an orchestration script.
    queue = create_fyi_data_mart(engine, active_projects_only=True)

    # equivalent SQL query that I translated to pandas to avoid dependency
    # on the data_mart schema (which doesn't yet exist when this function runs)
    """
    SELECT
        county_id_fips,
        resource_clean as resource_or_sector,
        count(project_id) as facility_count,
        sum(co2e_tonnes_per_year * frac_locations_in_county) as co2e_tonnes_per_year,
        sum(capacity_mw::float * frac_locations_in_county) as capacity_mw,
        'power plant' as facility_type,
        'proposed' as status
    from data_mart.fyi_projects_long_format
    where county_id_fips is not null -- 9 rows as of 6/4/2023
    group by 1, 2
    ;
    """
    # Distribute project-level quantities across locations, when there are multiple.
    # A handful of ISO projects are in multiple counties and the proprietary offshore
    # wind projects have an entry for each cable landing.
    # This approximation assumes an equal distribution between sites.
    # Also note that this model represents everything relevant to each county,
    # so multi-county projects are intentionally double-counted; for each relevant county.
    queue.loc[:, ["capacity_mw", "co2e_tonnes_per_year"]] = queue.loc[
        :, ["capacity_mw", "co2e_tonnes_per_year"]
    ].mul(queue["frac_locations_in_county"], axis=0)

    grp = queue.groupby(["county_id_fips", "resource_clean"])
    aggs = grp.agg(
        {
            "co2e_tonnes_per_year": "sum",  # type: ignore
            "capacity_mw": "sum",
            "project_id": "count",
        }
    )
    aggs.loc[:, "co2e_tonnes_per_year"] = aggs.loc[:, "co2e_tonnes_per_year"].replace(
        0,
        np.nan,
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


def _add_derived_columns(mart: pd.DataFrame) -> pd.DataFrame:
    out = mart.copy()
    out["ordinance_via_reldi"] = out["ordinance_text"].notna()
    priority_ban = mart["ordinance_via_self_maintained"]
    secondary_ban_cols = [
        "ordinance_via_reldi",
        "ordinance_via_solar_nrel",
        "ordinance_via_wind_nrel",
    ]
    out["ordinance_is_restrictive"] = priority_ban.fillna(
        mart[secondary_ban_cols].fillna(False).any(axis=1)
    )

    return out


def _convert_long_to_wide(long_format: pd.DataFrame) -> pd.DataFrame:
    # I try to source as much wide-format content as possible from the long-format data.
    # This is to reduce the number of places that need to be updated when the data changes.
    # The tradeoff for that reduced maintenance is the complexity of this function.
    long = long_format.copy()
    resources_to_keep = {
        "Battery Storage",
        "Solar",
        "Natural Gas",  # this name is shared between both power and infra
        # "Nuclear",
        "Onshore Wind",
        # "CSP",
        # "Other",
        # "Unknown",
        # "Biomass",
        # "Geothermal",
        # "Other Storage",
        "Offshore Wind",
        # "Hydro",
        # "Pumped Storage",
        "Coal",
        "Oil",  # this name is shared between both power and infra
        "Liquefied Natural Gas",
        "Ammonia and Synthetic Fertilizers",
        "Petrochemicals and Plastics",
    }
    to_keep = long["resource_or_sector"].isin(resources_to_keep)
    long = long.loc[to_keep, :]

    # prep values that will become part of column names after pivoting
    long.loc[:, "facility_type"] = long.loc[:, "facility_type"].map(
        {"fossil infrastructure": "infra", "power plant": ""}
    )
    long.loc[:, "resource_or_sector"] = long.loc[:, "resource_or_sector"].replace(
        {
            "Natural Gas": "gas",
            "Liquefied Natural Gas": "lng",
            "Ammonia and Synthetic Fertilizers": "ammonia_synth_fertilizers",
        }
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
    # county level cols are not part of the pivot and get dropped here.
    # They are added back in with _join_all_counties_to_wide_format()
    # because that function contains all counties instead of only
    # counties with power infrastructure in them

    wide = long.pivot(index=idx_cols, columns=col_cols, values=val_cols)
    wide.reset_index(inplace=True)

    wide.columns = wide.columns.rename(
        {None: "measures"}
    )  # value columns: capacity, count, etc
    wide.columns = wide.columns.reorder_levels(col_cols + ["measures"])
    wide.columns = [
        "_".join(col).strip("_") for col in wide.columns
    ]  # flatten multiindex

    # co2e total
    co2e_cols_to_sum = [
        col for col in wide.columns if col.endswith("co2e_tonnes_per_year")
    ]
    wide["county_total_co2e_tonnes_per_year"] = wide.loc[:, co2e_cols_to_sum].sum(
        axis=1
    )

    # fossil and renewable category totals
    renewables = [type_.lower().replace(" ", "_") for type_ in RENEWABLE_TYPES]
    fossils = [type_.lower().replace(" ", "_") for type_ in FOSSIL_TYPES]
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
        "ammonia_synth_fertilizers",
    ]
    for measure in measures:
        infra_cols_to_sum = [f"infra_{sector}_{measure}" for sector in sectors]
        wide[f"infra_total_{measure}"] = wide.loc[:, infra_cols_to_sum].sum(axis=1)

    wide.dropna(axis=1, how="all", inplace=True)
    cols_to_drop = [
        # A handful of hybrid facilities with co-located diesel generators.
        # They produce tiny amounts of CO2 but large amounts of confusion.
        "onshore_wind_existing_co2e_tonnes_per_year",
        "renewable_and_battery_proposed_co2e_tonnes_per_year",  # not currently modeled
        # No superset proposed_facility_counts due to double-counting multi-resource projects.
        # I recalculate those from the project data in _get_category_project_counts()
        "renewable_and_battery_proposed_facility_count",
        "fossil_proposed_facility_count",
    ]
    # some columns pop in and out of existence based on minor fluctuations in the data
    cols_to_drop = [col for col in cols_to_drop if col in wide.columns]
    wide.drop(columns=cols_to_drop, inplace=True)

    return wide


def create_long_format(
    postgres_engine: sa.engine.Engine,
) -> pd.DataFrame:
    """Create the long format county datamart dataframe."""
    all_counties = _get_county_fips_df(postgres_engine)
    all_states = _get_state_fips_df(postgres_engine)
    county_properties = _get_county_properties(postgres_engine=postgres_engine)
    projects = _fyi_projects_counties(postgres_engine)
    infra = _fossil_infrastructure_counties(postgres_engine)
    existing = _existing_plants_counties(
        postgres_engine=postgres_engine,
        state_fips_table=all_states,
        county_fips_table=all_counties,
    )

    # join it all
    out = pd.concat([projects, existing, infra], axis=0, ignore_index=True)
    out = out.merge(county_properties, on="county_id_fips", how="left")
    out = _add_derived_columns(out)
    return out


def _get_offshore_wind_extra_cols(engine: sa.engine.Engine) -> pd.DataFrame:
    # create columns that count how much offshore wind capacity is associated
    # with a particular county:
    #   1. comes from ports (m:m)
    #   2. is attributed to a particular interest type (m:1)

    # Note that these aggregates do NOT divide capacity across the counties. Instead,
    # they intentionally double-count capacity. The theory is that the loss
    # of any port could block the whole associated project, so we want
    # to know how much total capacity is at stake in each port county.
    query = get_query("get_offshore_wind_extra_cols.sql")
    df = pd.read_sql(query, engine)
    df.set_index("county_id_fips", inplace=True)
    return df


def _get_federal_land_fraction(postgres_engine: sa.engine.Engine):
    query = """
    select
        county_id_fips,
        gap_status,
        manager_type,
        intersection_area_padus_km2,
        county_area_coast_clipped_km2
    from data_warehouse.protected_area_by_county
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
    # NOTE: this query contains hardcoded parameters for the
    # energy communities qualification criteria
    query = get_query("get_energy_community_qualification.sql")
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
    energy_community_counties = _get_energy_community_qualification(postgres_engine)
    # model local opposition
    aggregator = CountyOpposition(
        engine=postgres_engine, county_fips_df=all_counties, state_fips_df=all_states
    )
    combined_opp = aggregator.agg_to_counties(
        include_state_policies=include_state_policies,
        include_nrel_bans=True,
        include_manual_ordinances=True,
    )

    county_properties = all_counties[
        [
            "county_name",
            "county_id_fips",
            "state_id_fips",
            "land_area_km2",
            "tribal_land_frac",
        ]
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
        energy_community_counties, on="county_id_fips", how="left", validate="1:1"
    )
    #  EC data currently only includes counties that have qualifying features.
    #  Fill in nulls for counties that do not qualify.
    county_properties.fillna(
        {
            "energy_community_coal_closures_area_fraction": 0.0,
            "energy_community_qualifies_via_employment": False,
            "energy_community_qualifies": False,
        },
        inplace=True,
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

    county_properties = county_properties.merge(
        wide_format,
        on="county_id_fips",
        how="left",
    )
    return county_properties


def _get_actionable_aggs_for_wide_format(engine: sa.engine.Engine) -> pd.DataFrame:
    """Create aggregates of renewables filtered by is_actionable and is_nearly_certain."""
    # Had to do this separately because including the is_actionable condition in
    # the long format data would be too ugly.
    fyi = create_fyi_data_mart(engine)
    fyi = _add_avoided_co2e(fyi, engine)

    # Distribute project-level quantities across locations, when there are multiple.
    # A handful of projects are in multiple counties and the proprietary offshore
    # wind projects have an entry for each cable landing.
    # This approximation assumes an equal distribution.
    fyi.loc[:, "capacity_mw"] = fyi.loc[:, "capacity_mw"].mul(
        fyi["frac_locations_in_county"]
    )
    fyi.loc[:, "avoided_co2e_tonnes_per_year"] = fyi.loc[
        :, "avoided_co2e_tonnes_per_year"
    ].mul(fyi["frac_locations_in_county"])

    resources = ["Onshore Wind", "Offshore Wind", "Solar", "renewable_and_battery"]
    aggs = []
    for resource in resources:
        if resource == "renewable_and_battery":
            resource_filter = fyi["resource_clean"].isin(set(RENEWABLE_TYPES))
        else:
            resource_filter = fyi["resource_clean"] == resource
        resource_name = resource.lower().replace(" ", "_")
        for status in ["actionable", "nearly_certain"]:
            filter_ = resource_filter & fyi[f"is_{status}"]
            rename_dict = {
                "capacity_mw": f"{resource_name}_proposed_capacity_mw_{status}",
                "project_id": f"{resource_name}_proposed_facility_count_{status}",
                "avoided_co2e_tonnes_per_year": f"{resource_name}_proposed_avoided_co2e_{status}",
            }
            agg = (
                fyi.loc[filter_, :]
                .groupby("county_id_fips")
                .agg(
                    {
                        "capacity_mw": "sum",
                        "project_id": "nunique",  # "count" would over-count multi-resource projects
                        "avoided_co2e_tonnes_per_year": "sum",
                    }
                )
            )
            agg.rename(columns=rename_dict, inplace=True)
            aggs.append(agg)
        # and avoided co2 totals. This doesn't belong in this function but c'est la vie.
        agg = (
            fyi.loc[resource_filter, :]
            .groupby("county_id_fips")["avoided_co2e_tonnes_per_year"]
            .sum()
            .rename(f"{resource_name}_proposed_avoided_co2e_tonnes_per_year")
        )
        aggs.append(agg)

    aggs = pd.concat(aggs, axis=1, join="outer")

    return aggs


def _get_actionable_aggs_for_long_format(engine: sa.engine.Engine) -> pd.DataFrame:
    """Calculate fraction of MW considered actionable."""
    # This should be refactored and combined with the wide format version above.
    fyi = create_fyi_data_mart(engine)

    # Distribute project-level quantities across locations, when there are multiple.
    # A handful of projects are in multiple counties and the proprietary offshore
    # wind projects have an entry for each cable landing.
    # This approximation assumes an equal distribution.
    fyi["capacity_mw"] = fyi.loc[:, "capacity_mw"].mul(fyi["frac_locations_in_county"])
    grp = fyi.groupby(["county_id_fips", "resource_clean", "is_actionable"])
    actionable_sums = grp["capacity_mw"].sum().unstack(level=-1)
    frac_actionable = (
        actionable_sums[True].div(actionable_sums.sum(axis=1), axis=0).to_frame()
    )
    frac_actionable["facility_type"] = "power plant"
    frac_actionable["status"] = "proposed"
    frac_actionable.reset_index(inplace=True)
    frac_actionable.rename(
        columns={0: "actionable_mw_fraction", "resource_clean": "resource_or_sector"},
        inplace=True,
    )

    return frac_actionable


def _add_avoided_co2e(iso: pd.DataFrame, engine: sa.engine.Engine) -> pd.DataFrame:
    emiss_fac_by_county = _get_avoided_emissions_by_county_resource(engine)
    emiss_fac_by_county["resource_type"].replace(
        {
            "onshore_wind": "Onshore Wind",
            "offshore_wind": "Offshore Wind",
            "utility_pv": "Solar",
        },
        inplace=True,
    )
    emiss_fac_by_county.rename(
        columns={"resource_type": "resource_clean"}, inplace=True
    )

    iso = iso.merge(
        emiss_fac_by_county, on=["county_id_fips", "resource_clean"], how="left"
    )
    iso["avoided_co2e_tonnes_per_year"] = (
        iso["capacity_mw"] * iso["co2e_tonnes_per_year_per_mw"]
    )
    return iso


def _get_avoided_emissions_by_county_resource(engine: sa.engine.Engine) -> pd.DataFrame:
    emiss_fac = pd.read_sql_table(
        "avert_avoided_emissions_factors", engine, schema="data_warehouse"
    )
    emiss_fac = emiss_fac[
        emiss_fac.resource_type.isin(["onshore_wind", "offshore_wind", "utility_pv"])
    ]
    emiss_fac = emiss_fac[
        ["avert_region", "resource_type", "co2e_tonnes_per_year_per_mw"]
    ]
    crosswalk = pd.read_sql_table(
        "avert_county_region_assoc", engine, schema="data_warehouse"
    )
    emiss_fac_by_county = crosswalk.merge(emiss_fac, on="avert_region", how="left")

    # fill in AK and HI with national averages
    national_avgs = (
        emiss_fac.groupby("resource_type")["co2e_tonnes_per_year_per_mw"]
        .mean()
        .rename("avg_co2")
    )
    emiss_fac_by_county = emiss_fac_by_county.merge(
        national_avgs, on="resource_type", how="left"
    )
    emiss_fac_by_county["co2e_tonnes_per_year_per_mw"].fillna(
        emiss_fac_by_county["avg_co2"], inplace=True
    )
    emiss_fac_by_county.drop(columns=["avg_co2", "avert_region"], inplace=True)
    return emiss_fac_by_county


def create_wide_format(
    postgres_engine: Optional[sa.engine.Engine] = None,
    long_format: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Create wide format county aggregates."""
    if postgres_engine is None:
        postgres_engine = get_sql_engine()
    if long_format is None:
        long_format = create_long_format(postgres_engine=postgres_engine)
    wide_format = _convert_long_to_wide(long_format)
    # add aggregates that have to be recreated from the project-level data
    proposed_counts = _get_category_project_counts(postgres_engine)
    # client requested joining all counties onto wide format table, even if all values are NULL
    county_properties = _get_county_properties(postgres_engine)
    wide_format = _join_all_counties_to_wide_format(wide_format, county_properties)
    # client requested two additional columns relating to offshore wind
    offshore_bits = _get_offshore_wind_extra_cols(postgres_engine)
    actionable_bits = _get_actionable_aggs_for_wide_format(postgres_engine)

    wide_format = pd.concat(
        [
            wide_format.set_index("county_id_fips"),
            proposed_counts,
            offshore_bits,
            actionable_bits,
        ],
        axis=1,
        join="outer",
    )
    wide_format.reset_index(inplace=True)
    assert (
        wide_format["renewable_and_battery_proposed_facility_count"]
        .fillna(0)
        .ge(
            wide_format[
                "renewable_and_battery_proposed_facility_count_actionable"
            ].fillna(0)
        )
        .all()
    ), "actionable count should be less than or equal to total count"
    return wide_format


def _get_category_project_counts(engine: sa.engine.Engine) -> pd.DataFrame:
    """Count projects by resource class.

    Necessary because aggregating by resource_class in the long format would
    double-count projects that have multiple resources, like Solar + Storage.
    """
    # Avoid db dependency order by recreating the df.
    # Could also make an orchestration script.
    fyi = create_fyi_data_mart(engine)
    fyi["surrogate_project_id"] = fyi["project_id"].astype(str) + fyi["source"]
    # equivalent SQL query that I translated to pandas to avoid dependency
    # on the data_mart schema (which doesn't yet exist when this function runs)
    """
    SELECT
        county_id_fips,
        count(DISTINCT (project_id, source)) as facility_count,
    FROM data_mart.fyi_projects_long_format
    WHERE resource_clean IN ('Solar', 'Offshore Wind', 'Onshore Wind', 'Battery Storage')
    -- and repeat for fossil types
    group by 1
    ;
    """
    is_renewable_and_battery = fyi["resource_clean"].isin(set(RENEWABLE_TYPES))
    is_fossil = fyi["resource_clean"].isin(set(FOSSIL_TYPES))
    aggs = []
    for name, filter_ in {
        "renewable_and_battery": is_renewable_and_battery,
        "fossil": is_fossil,
    }.items():
        agg = (
            fyi.loc[filter_, :]
            .groupby("county_id_fips")["surrogate_project_id"]
            .nunique()  # "count" would over-count multi-resource projects
            .rename(f"{name}_proposed_facility_count")
        )
        aggs.append(agg)
    aggs = pd.concat(aggs, axis=1, join="outer")  # fillna(0) later, after all joins
    return aggs


def create_data_mart(
    engine: Optional[sa.engine.Engine] = None,
) -> Dict[str, pd.DataFrame]:
    """Create county data marts.

    Args:
        engine (Optional[sa.engine.Engine], optional): postgres engine. Defaults to None.

    Returns:
        Dict[str, pd.DataFrame]: county tables in both wide and long format
    """
    postgres_engine = engine
    if postgres_engine is None:
        postgres_engine = get_sql_engine()

    long_format = create_long_format(postgres_engine=postgres_engine)
    wide_format = create_wide_format(
        postgres_engine=postgres_engine,
        long_format=long_format,
    )
    actionable_col = _get_actionable_aggs_for_long_format(postgres_engine)
    long_format = long_format.merge(
        actionable_col,
        on=["county_id_fips", "resource_or_sector", "facility_type", "status"],
        how="left",
    )

    out = {
        "counties_long_format": long_format,
        "counties_wide_format": wide_format,
    }
    return out


if __name__ == "__main__":
    # debugging entry point
    engine = get_sql_engine()
    marts = create_data_mart(engine=engine)

    print("hooray")
