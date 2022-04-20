"""Module to create a table for the Carbon, EJ, and Equity Dashboard in Tabealu.

The table is at the county level and contains data from.

"""
from typing import Optional

import pandas as pd
import sqlalchemy as sa

from dbcp.helpers import get_sql_engine


def _get_existing_fossil_plants(engine: sa.engine.Engine) -> pd.DataFrame:
    # see last SELECT statement for output columns
    query = """
    WITH
    projects as (
        SELECT
            county_id_fips,
            state_id_fips,
            plant_id_eia,
            generator_id,
            prime_mover_code,
            fuel_type_code_pudl,
            total_mmbtu,
            net_generation_mwh,
            capacity_mw,
            latitude,
            longitude
        FROM data_warehouse.mcoe
        WHERE operational_status = 'existing'
        AND fuel_type_code_pudl in ('gas', 'oil', 'coal')
    ),
    w_county_names as (
    select
        cfip.county_name as county,
        proj.*
    from projects as proj
    left join data_warehouse.county_fips as cfip
        on proj.county_id_fips = cfip.county_id_fips
    ),
    w_names as (
        SELECT
            sfip.state_name as state,
            proj.*
        from w_county_names as proj
        left join data_warehouse.state_fips as sfip
            on proj.state_id_fips = sfip.state_id_fips
    )
    select
        proj.*,
        ncsl.permitting_type
    from w_names as proj
    left join data_warehouse.ncsl_state_permitting as ncsl
        on proj.state_id_fips = ncsl.state_id_fips
    ;

    """
    df = pd.read_sql(query, engine)
    return df


def _add_emissions_factors(df: pd.DataFrame) -> None:
    # https://www.ecfr.gov/current/title-40/chapter-I/subchapter-C/part-98/subpart-C/appendix-Table%20C-1%20to%20Subpart%20C%20of%20Part%2098
    epa_emissions_factors = {  # from EPA 40 CFR 98. kg CO2e/mmbtu
        "gas": 53.06,
        "coal": 95.52,  # power sector average
        "oil": 73.96,  # DFO #2
    }
    df["kg_co2_per_mmbtu"] = df.loc[:, "fuel_type_code_pudl"].map(epa_emissions_factors)


def _estimate_co2e_from_fuel_burn(df: pd.DataFrame) -> None:
    """Estimate CO2e from fuel reporting. IT IS MISSING ALL GAS TURBINES.

    This accounts for 88% of net generation, but only 27% of generators.
    """
    _add_emissions_factors(df)
    kg_per_tonne = 1000
    df["co2e_tonnes_per_year"] = (
        df.loc[:, "total_mmbtu"] * df.loc[:, "kg_co2_per_mmbtu"] / kg_per_tonne
    )
    return


def _estimate_co2e_from_net_generation(df: pd.DataFrame) -> None:
    # https://www.eia.gov/electricity/annual/html/epa_08_02.html
    heat_rates = {
        "coal_st": 9997.0,
        "gas_st": 10368.0,
        "gas_gt": 11069.0,
        "gas_cc": 7604.0,
        "oil_ic": 10334.0,
    }
    raise NotImplementedError


def _harmonize_fuels_with_iso_queue(
    df: pd.DataFrame, fuel_type_col: str = "resource"
) -> None:
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
    df.loc[:, fuel_type_col] = df.loc[:, fuel_type_col].map(resource_map)


def _dummy_existing_co2_table(engine: sa.engine.Engine) -> pd.DataFrame:
    df = _get_existing_fossil_plants(engine=engine)
    _estimate_co2e_from_fuel_burn(df)
    groups = df.groupby("plant_id_eia")
    categorical_cols = [
        "state",
        "county",
        "county_id_fips",
        "state_id_fips",
        "fuel_type_code_pudl",
        "latitude",
        "longitude",
        "permitting_type",
    ]
    quantitative_cols = [
        "total_mmbtu",
        "net_generation_mwh",
        "capacity_mw",
        "co2e_tonnes_per_year",
    ]
    plant_data = groups[categorical_cols].nth(0)
    quant_data = groups[quantitative_cols].sum()
    plant_data = plant_data.join(quant_data).reset_index()
    plant_data["facility_type"] = "existing_power"
    plant_data.rename(
        columns={"plant_id_eia": "id", "fuel_type_code_pudl": "resource"},
        inplace=True,
    )
    _harmonize_fuels_with_iso_queue(plant_data)
    plant_data.drop(
        columns=[
            "total_mmbtu",
            "net_generation_mwh",
        ],
        inplace=True,
    )
    return plant_data


def _get_proposed_fossil_plants(engine: sa.engine.Engine) -> pd.DataFrame:
    # see last SELECT statement for output columns
    query = """
    WITH
    active_loc as (
        select
            proj.project_id,
            loc.county_id_fips
        from data_warehouse.iso_projects as proj
        left join data_warehouse.iso_locations as loc
            on loc.project_id = proj.project_id
        where proj.queue_status = 'active'
    ),
    projects as (
        select
            loc.project_id,
            loc.county_id_fips,
            res.capacity_mw,
            res.resource_clean as resource
        from active_loc as loc
        left join data_warehouse.iso_resource_capacity as res
            on res.project_id = loc.project_id
        where res.capacity_mw is not NULL
        and res.resource_clean in ('Natural Gas', 'Coal', 'Oil')
    ),
    w_county_names as (
    select
        cfip.county_name as county,
        cfip.state_id_fips,
        proj.*
    from projects as proj
    left join data_warehouse.county_fips as cfip
        on proj.county_id_fips = cfip.county_id_fips
    ),
    w_names as (
        SELECT
            sfip.state_name as state,
            proj.*
        from w_county_names as proj
        left join data_warehouse.state_fips as sfip
            on proj.state_id_fips = sfip.state_id_fips
    )
    select
        ncsl.permitting_type,
        proj.*
    from w_names as proj
    left join data_warehouse.ncsl_state_permitting as ncsl
        on proj.state_id_fips = ncsl.state_id_fips
    ;
    """
    df = pd.read_sql(query, engine)
    df["co2e_tonnes_per_year"] = (
        df.loc[:, "capacity_mw"] * 8766 * 0.5 * 8 * 53.06 / 1000
    )
    df.rename(columns={"project_id": "id"}, inplace=True)
    df["facility_type"] = "proposed_power"
    return df


def _get_proposed_fossil_infra(engine: sa.engine.Engine) -> pd.DataFrame:
    query = """
    WITH
    projects as (
        SELECT
            project_id,
            greenhouse_gases_co2e_tpy * 0.907185 as co2e_tonnes_per_year
        FROM data_warehouse.eip_projects
        WHERE operating_status not in ('Operating', 'Under construction', 'Canceled')
    ),
    facilities as (
        SELECT
            facility_id,
            county_id_fips,
            state_id_fips,
            latitude,
            longitude,
            "raw_percent_low-income_within_3_miles" as low_income_pct,
            raw_percent_people_of_color_within_3_miles as people_of_color_pct
        FROM data_warehouse.eip_facilities
    ),
    association as (
        -- this query simplifies the m:m relationship
        -- by taking only the first result, making it m:1.
        -- Only 5 rows are dropped.
        select DISTINCT ON (project_id)
            project_id,
            facility_id
        from data_warehouse.eip_facility_project_association
        order by 1, 2 DESC
    ),
    proj_agg_to_facility as (
        SELECT
            ass.facility_id,
            sum(co2e_tonnes_per_year) as co2e_tonnes_per_year
        FROM projects as proj
        LEFT JOIN association as ass
        ON proj.project_id = ass.project_id
        GROUP BY 1
    ),
    facility_aggs as (
        SELECT
            fac.*,
            proj.co2e_tonnes_per_year
        FROM proj_agg_to_facility as proj
        LEFT JOIN facilities as fac
        ON proj.facility_id = fac.facility_id
    ),
    w_county_names as (
    select
        cfip.county_name as county,
        proj.*
    from facility_aggs as proj
    left join data_warehouse.county_fips as cfip
        on proj.county_id_fips = cfip.county_id_fips
    ),
    w_names as (
        SELECT
            sfip.state_name as state,
            proj.*
        from w_county_names as proj
        left join data_warehouse.state_fips as sfip
            on proj.state_id_fips = sfip.state_id_fips
    )
    select
        proj.*,
        ncsl.permitting_type
    from w_names as proj
    left join data_warehouse.ncsl_state_permitting as ncsl
        on proj.state_id_fips = ncsl.state_id_fips
    ;
    """
    df = pd.read_sql(query, engine)
    df["facility_type"] = "proposed_infrastructure"
    df.rename(columns={"facility_id": "id"}, inplace=True)
    return df


def create_data_mart(engine: Optional[sa.engine.Engine] = None) -> pd.DataFrame:
    if engine is None:
        engine = get_sql_engine()
    tables = [
        func(engine=engine)
        for func in (
            _dummy_existing_co2_table,
            _get_proposed_fossil_plants,
            _get_proposed_fossil_infra,
        )
    ]
    df = pd.concat(tables, axis=0, ignore_index=True, copy=False)
    return df
