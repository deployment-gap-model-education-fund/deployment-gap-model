"""Module to create a table for the Carbon, EJ, and Equity Dashboard in Tabealu.

The table is at the county level and contains data from.

"""
from typing import Optional

import pandas as pd
import sqlalchemy as sa

from dbcp.data_mart.helpers import _get_county_fips_df, _get_state_fips_df
from dbcp.helpers import download_pudl_data, get_sql_engine
from dbcp.transform.helpers import add_county_fips_with_backup_geocoding


def _get_existing_plant_fuel_data(pudl_engine: sa.engine.Engine) -> pd.DataFrame:
    query = """
    select
        plant_id_eia,
        fuel_type_code_pudl,
        prime_mover_code,
        report_date,
        net_generation_mwh as mwh,
        fuel_consumed_for_electricity_mmbtu as mmbtu
    from generation_fuel_eia923
    where report_date >= date('2020-01-01') -- this is monthly data
    AND fuel_type_code_pudl in ('coal', 'gas', 'oil')
    ;
    """
    df = pd.read_sql(query, pudl_engine)
    return df


def _combine_cc_parts(gen_fuel_923: pd.DataFrame) -> pd.DataFrame:
    # split, apply, combine
    # split
    is_cc = gen_fuel_923.loc[:, "prime_mover_code"].isin(
        {
            "CA",
            "CT",
        }
    )
    cc = gen_fuel_923.loc[is_cc, :]
    # apply
    cc = cc.groupby(
        ["plant_id_eia", "report_date", "fuel_type_code_pudl"], as_index=False
    )[["mwh", "mmbtu"]].sum()
    cc["prime_mover_code"] = "CC"
    # combine
    non_cc = gen_fuel_923.loc[~is_cc, :]
    out = pd.concat([cc, non_cc], axis=0, ignore_index=True)
    return out


def _add_emissions_factors(gen_fuel_923: pd.DataFrame) -> None:
    # https://www.ecfr.gov/current/title-40/chapter-I/subchapter-C/part-98/subpart-C/appendix-Table%20C-1%20to%20Subpart%20C%20of%20Part%2098
    # from EPA 40 CFR 98. I divide by 1000 to convert kg_CO2e/mmbtu to metric tonnes
    epa_emissions_factors = {
        "gas": 53.06 / 1000,
        "coal": 95.52 / 1000,  # power sector average
        "oil": 73.96 / 1000,  # DFO #2
    }
    gen_fuel_923["tonnes_co2_per_mmbtu"] = gen_fuel_923.loc[
        :, "fuel_type_code_pudl"
    ].map(epa_emissions_factors)


def _co2_from_mmbtu(
    gen_fuel_923: pd.DataFrame,
    output_name: str = "co2e_tonnes_via_mmbtu",
    mmbtu_col: str = "mmbtu",
) -> None:
    """Estimate CO2e from fuel reporting."""
    if "tonnes_co2_per_mmbtu" not in gen_fuel_923.columns:
        _add_emissions_factors(gen_fuel_923)
    gen_fuel_923[output_name] = (
        gen_fuel_923.loc[:, mmbtu_col] * gen_fuel_923.loc[:, "tonnes_co2_per_mmbtu"]
    )
    return


def _co2_from_mwh(
    df: pd.DataFrame,
    output_name: str = "co2e_tonnes_via_mwh",
    mwh_col: str = "mwh",
) -> None:
    # https://www.eia.gov/electricity/annual/html/epa_08_02.html
    heat_rates_mmbtu_per_mwh = {
        "coal_CC": 9.997,  # CC = my made up prime mover code!
        "coal_GT": 9.997,
        "coal_IC": 9.997,
        "coal_OT": 9.997,
        "coal_ST": 9.997,
        "gas_ST": 10.368,
        "gas_OT": 10.368,
        "gas_IC": 8.832,
        "gas_GT": 11.069,
        "gas_FC": 7.604,
        "gas_CC": 7.604,  # CC = my made up prime mover code!
        "gas_CS": 7.604,
        "gas_CE": 7.604,
        "oil_CC": 9.208,  # CC = my made up prime mover code!
        "oil_CS": 9.208,
        "oil_GT": 13.223,
        "oil_IC": 10.334,
        "oil_ST": 10.339,
    }
    if "tonnes_co2_per_mmbtu" not in df.columns:
        _add_emissions_factors(df)
    df["fuel_prime_mover"] = (
        df.loc[:, "fuel_type_code_pudl"] + "_" + df.loc[:, "prime_mover_code"]
    )
    df["mmbtu_per_mwh"] = df.loc[:, "fuel_prime_mover"].map(heat_rates_mmbtu_per_mwh)

    df[output_name] = (
        df.loc[:, mwh_col]
        * df.loc[:, "mmbtu_per_mwh"]
        * df.loc[:, "tonnes_co2_per_mmbtu"]
    )
    intermediates = ["tonnes_co2_per_mmbtu", "mmbtu_per_mwh", "fuel_prime_mover"]
    df.drop(columns=intermediates, inplace=True)
    return


def _estimate_existing_co2e(gen_fuel_923: pd.DataFrame) -> pd.DataFrame:
    co2e = _combine_cc_parts(gen_fuel_923)
    _co2_from_mmbtu(co2e)
    _co2_from_mwh(co2e)
    # use mmbtu calc unless it is zero, in which case use the max estimate.
    condition = co2e.loc[:, "co2e_tonnes_via_mmbtu"].gt(0)
    alternative = co2e.loc[:, ["co2e_tonnes_via_mmbtu", "co2e_tonnes_via_mwh"]].max(
        axis=1
    )
    co2e["co2e_tonnes_per_year"] = co2e.loc[:, "co2e_tonnes_via_mmbtu"].where(
        condition, other=alternative
    )
    plant_co2e = co2e.groupby("plant_id_eia")["co2e_tonnes_per_year"].sum()
    return plant_co2e


def _get_plant_location_data(pudl_engine: sa.engine.Engine) -> pd.DataFrame:
    query = """
    select
        plant_id_eia,
        state,
        county
    from plants_entity_eia
    ;
    """
    df = pd.read_sql(query, pudl_engine)
    return df


def _transfrom_plant_location_data(
    plant_locations: pd.DataFrame, state_table: pd.DataFrame, county_table: pd.DataFrame
) -> pd.DataFrame:
    plant_locations = add_county_fips_with_backup_geocoding(
        plant_locations, state_col="state", locality_col="county"
    )
    plant_locations = plant_locations.merge(
        state_table.loc[:, ["state_id_fips", "state_name"]],
        on="state_id_fips",
        how="left",
        copy=False,
    )
    plant_locations = plant_locations.merge(
        county_table.loc[:, ["county_id_fips", "county_name"]],
        on="county_id_fips",
        how="left",
        copy=False,
    )
    plant_locations.drop(
        columns=[
            "geocoded_locality_name",
            "geocoded_locality_type",
            "geocoded_containing_county",
            "state",
            "county",
        ],
        inplace=True,
    )
    plant_locations.rename(
        columns={"state_name": "state", "county_name": "county"}, inplace=True
    )
    return plant_locations


def _get_existing_fossil_plants(
    postgres_engine: sa.engine.Engine,
    pudl_engine: sa.engine.Engine,
) -> pd.DataFrame:
    gen_fuel_923 = _get_existing_plant_fuel_data(pudl_engine)
    plant_co2e = _estimate_existing_co2e(gen_fuel_923)
    states = _get_state_fips_df(postgres_engine)
    counties = _get_county_fips_df(postgres_engine)
    plant_locations = _get_plant_location_data(pudl_engine)
    plant_locations = _transfrom_plant_location_data(
        plant_locations, state_table=states, county_table=counties
    )

    plant_data = plant_locations.merge(
        plant_co2e, on="plant_id_eia", how="right", copy=False
    )
    plant_data["facility_type"] = "existing_power"
    plant_data.rename(
        columns={"plant_id_eia": "id"},
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
    )
    SELECT
        sfip.state_name as state,
        proj.*
    from w_county_names as proj
    left join data_warehouse.state_fips as sfip
        on proj.state_id_fips = sfip.state_id_fips
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
            -- First multiplier below is unit conversion
            -- The second is 15 percent haircut to account for realistic utilization, as per design doc.
            greenhouse_gases_co2e_tpy * 0.907185 * 0.85 as co2e_tonnes_per_year
        FROM data_warehouse.eip_projects
        WHERE operating_status not in ('Operating', 'Under construction', 'Canceled')
    ),
    facilities as (
        SELECT
            facility_id,
            county_id_fips,
            state_id_fips
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
    )
    SELECT
        sfip.state_name as state,
        proj.*
    from w_county_names as proj
    left join data_warehouse.state_fips as sfip
        on proj.state_id_fips = sfip.state_id_fips
    ;
    """
    df = pd.read_sql(query, engine)
    df["facility_type"] = "proposed_infrastructure"
    df.rename(columns={"facility_id": "id"}, inplace=True)
    return df


def create_data_mart(
    postgres_engine: Optional[sa.engine.Engine] = None,
    pudl_engine: Optional[sa.engine.Engine] = None,
) -> pd.DataFrame:
    """Create final output table.

    Args:
        engine (Optional[sa.engine.Engine], optional): sqlalchemy engine. Defaults to None.

    Returns:
        pd.DataFrame: table for data mart
    """
    if postgres_engine is None:
        postgres_engine = get_sql_engine()
    if pudl_engine is None:
        pudl_data_path = download_pudl_data()
        pudl_engine = sa.create_engine(
            f"sqlite:////{pudl_data_path}/pudl_data/sqlite/pudl.sqlite"
        )

    tables = [
        func(engine=postgres_engine)
        for func in (
            _get_proposed_fossil_plants,
            _get_proposed_fossil_infra,
        )
    ]
    tables.append(
        _get_existing_fossil_plants(
            postgres_engine=postgres_engine, pudl_engine=pudl_engine
        )
    )
    df = pd.concat(tables, axis=0, ignore_index=True, copy=False)
    return df
