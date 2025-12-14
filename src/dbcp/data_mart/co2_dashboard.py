"""Module to create a table for the Carbon, EJ, and Equity Dashboard in Tabealu.

The table is at the county level and contains data from.

"""

import pandas as pd
import sqlalchemy as sa

from dbcp.constants import PUDL_LATEST_YEAR
from dbcp.data_mart.helpers import _get_county_fips_df, _get_state_fips_df, get_query
from dbcp.helpers import get_pudl_resource, get_sql_engine
from dbcp.transform.helpers import (
    add_county_fips_with_backup_geocoding,
    bedford_addfips_fix,
)


def _get_existing_plant_fuel_data() -> pd.DataFrame:
    df = pd.read_parquet(
        get_pudl_resource("core_eia923__monthly_generation_fuel.parquet"),
        engine="pyarrow",
        use_nullable_dtypes=True,
    )
    # convert all columns with the word date to datetime
    date_columns = [col for col in df.columns if "date" in col]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col])

    # convert all categorical columns to strings
    for col in df.select_dtypes(include="category").columns:
        df[col] = df[col].astype("string")
    df = df[
        (df["report_date"] >= f"{PUDL_LATEST_YEAR}-01-01")
        & (df["report_date"] < f"{PUDL_LATEST_YEAR + 1}-01-01")
        & (df["fuel_type_code_pudl"].isin(["coal", "gas", "oil"]))
    ]

    # Select specific columns
    df = df[
        [
            "plant_id_eia",
            "fuel_type_code_pudl",
            "prime_mover_code",
            "report_date",
            "net_generation_mwh",
            "fuel_consumed_for_electricity_mmbtu",
        ]
    ]

    # Rename columns
    df = df.rename(
        columns={
            "net_generation_mwh": "mwh",
            "fuel_consumed_for_electricity_mmbtu": "mmbtu",
        }
    )
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


def _add_emissions_factors(
    gen_fuel_923: pd.DataFrame, fuel_type_col: str = "fuel_type_code_pudl"
) -> None:
    # https://www.ecfr.gov/current/title-40/chapter-I/subchapter-C/part-98/subpart-C/appendix-Table%20C-1%20to%20Subpart%20C%20of%20Part%2098
    # from EPA 40 CFR 98. I divide by 1000 to convert kg_CO2e/mmbtu to metric tonnes
    epa_emissions_factors = {
        "gas": 53.06 / 1000,
        "coal": 95.52 / 1000,  # power sector average
        "oil": 73.96 / 1000,  # DFO #2
    }
    gen_fuel_923["tonnes_co2_per_mmbtu"] = gen_fuel_923.loc[:, fuel_type_col].map(
        epa_emissions_factors
    )


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


def _estimate_existing_co2e(gen_fuel_923: pd.DataFrame) -> pd.Series:
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


def _get_plant_location_data() -> pd.DataFrame:
    df = pd.read_parquet(get_pudl_resource("core_eia__entity_plants.parquet"))
    return df[["plant_id_eia", "state", "county"]]


def _transfrom_plant_location_data(
    plant_locations: pd.DataFrame, state_table: pd.DataFrame, county_table: pd.DataFrame
) -> pd.DataFrame:
    bedford_addfips_fix(plant_locations)
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
) -> pd.DataFrame:
    gen_fuel_923 = _get_existing_plant_fuel_data()
    plant_co2e = _estimate_existing_co2e(gen_fuel_923)
    states = _get_state_fips_df(postgres_engine)
    counties = _get_county_fips_df(postgres_engine)
    plant_locations = _get_plant_location_data()
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
    query = get_query("get_proposed_fossil_plants.sql")
    df = pd.read_sql(query, engine)
    _estimate_proposed_power_co2e(df)
    df.rename(columns={"project_id": "id"}, inplace=True)
    df["facility_type"] = "proposed_power"
    df.drop(columns=["capacity_mw", "resource"], inplace=True)
    return df


def _estimate_proposed_power_co2e(
    df: pd.DataFrame,
) -> None:
    """Estimate CO2e tons per year from capacity and fuel type.

    This is essentially a manual decision tree. Capacity factors were simple mean
    values derived from recent gas plants. See notebooks/12-tpb-revisit_co2_estimates.ipynb
    heat rate source: https://www.eia.gov/electricity/annual/html/epa_08_02.html
    emissions factor source: https://github.com/grgmiller/emission-factors (EPA Mandatory Reporting of Greenhouse Gases Rule)

    Args:
        df (pd.DataFrame): denormalized iso queue

    Returns:
        pd.DataFrame: copy of input dataframe with new column 'co2e_tonnes_per_year'

    """
    gas_turbine_mmbtu_per_mwh = 11.069
    combined_cycle_mmbtu_per_mwh = 7.604
    coal_steam_turbine_mmbtu_per_mwh = 9.997

    cc_gt_capacity_mw_split = 450.0
    gt_sub_split = 110

    cc_cap_factor = 0.622
    gt_large_cap_factor = 0.107
    gt_small_cap_factor = 0.608
    coal_cap_factor = 0.6

    df["mod_resource"] = df["resource"].map(
        {"Natural Gas": "gas", "Coal": "coal", "Oil": "oil"}
    )
    _add_emissions_factors(df, fuel_type_col="mod_resource")

    assert set(df.loc[:, "mod_resource"].unique()) == {"coal", "gas"}
    df["mmbtu_per_mwh"] = gas_turbine_mmbtu_per_mwh
    is_cc = df.loc[:, "capacity_mw"].gt(cc_gt_capacity_mw_split)
    is_coal = df.loc[:, "mod_resource"] == "coal"
    df.loc[:, "mmbtu_per_mwh"].where(
        ~is_cc, other=combined_cycle_mmbtu_per_mwh, inplace=True
    )
    df.loc[:, "mmbtu_per_mwh"].where(
        ~is_coal, other=coal_steam_turbine_mmbtu_per_mwh, inplace=True
    )

    df["estimated_capacity_factor"] = gt_small_cap_factor
    df.loc[:, "estimated_capacity_factor"].where(
        ~is_cc & df.loc[:, "capacity_mw"].le(gt_sub_split),
        other=gt_large_cap_factor,
        inplace=True,
    )
    df.loc[:, "estimated_capacity_factor"].where(
        ~is_cc, other=cc_cap_factor, inplace=True
    )
    df.loc[:, "estimated_capacity_factor"].where(
        ~is_coal, other=coal_cap_factor, inplace=True
    )

    # Put it all together
    hours_per_year = 8766  # extra 6 hours to average in leap years
    df["mwh"] = df["capacity_mw"] * df["estimated_capacity_factor"] * hours_per_year
    df["co2e_tonnes_per_year"] = (
        df["mwh"] * df["mmbtu_per_mwh"] * df["tonnes_co2_per_mmbtu"]
    )
    intermediates = [
        "tonnes_co2_per_mmbtu",
        "mmbtu_per_mwh",
        "mwh",
        "mod_resource",
        "estimated_capacity_factor",
    ]
    df.drop(columns=intermediates, inplace=True)
    return


def _get_proposed_fossil_infra(engine: sa.engine.Engine) -> pd.DataFrame:
    query = get_query("get_proposed_fossil_infra.sql")
    df = pd.read_sql(query, engine)
    df["facility_type"] = "proposed_infrastructure"
    df.rename(columns={"facility_id": "id"}, inplace=True)
    return df


def create_data_mart(
    engine: sa.engine.Engine | None = None,
) -> pd.DataFrame:
    """Create final output table.

    Args:
        engine (Optional[sa.engine.Engine], optional): postgres engine. Defaults to None.

    Returns:
        pd.DataFrame: table for data mart

    """
    postgres_engine = engine
    if postgres_engine is None:
        postgres_engine = get_sql_engine()

    tables = [
        func(engine=postgres_engine)
        for func in (
            _get_proposed_fossil_plants,
            _get_proposed_fossil_infra,
        )
    ]
    tables.append(_get_existing_fossil_plants(postgres_engine=postgres_engine))
    df = pd.concat(tables, axis=0, ignore_index=True, copy=False)
    return df
