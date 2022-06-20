"""Module to create a project-level table for DBCP to use in spreadsheet tools."""
from re import IGNORECASE
from typing import Optional

import numpy as np
import pandas as pd
import sqlalchemy as sa

from dbcp.data_mart.helpers import (
    CountyOpposition,
    _estimate_proposed_power_co2e,
    _get_county_fips_df,
    _get_state_fips_df,
)
from dbcp.helpers import get_sql_engine


def _get_and_join_iso_tables(engine: sa.engine.Engine) -> pd.DataFrame:
    query = """
    WITH
    proj_res as (
        SELECT
            proj.project_id,
            proj.date_proposed as date_proposed_online,
            proj.developer,
            proj.entity,
            proj.interconnection_status_lbnl as interconnection_status,
            proj.point_of_interconnection,
            proj.project_name,
            proj.queue_date as date_entered_queue,
            proj.queue_status,
            proj.region as iso_region,
            proj.utility,
            res.capacity_mw,
            res.resource_clean
        FROM data_warehouse.iso_projects_2021 as proj
        INNER JOIN data_warehouse.iso_resource_capacity_2021 as res
        ON proj.project_id = res.project_id
    ),
    loc as (
        -- this query simplifies the 1:m relationship
        -- by taking only the first result, making it 1:1.
        -- Only 26 / 13259 rows are dropped.
        SELECT DISTINCT ON (project_id)
            project_id,
            state_id_fips,
            county_id_fips
        FROM data_warehouse.iso_locations_2021
    ),
    iso as (
        SELECT
            proj_res.*,
            loc.state_id_fips,
            loc.county_id_fips
        from proj_res
        LEFT JOIN loc
        ON proj_res.project_id = loc.project_id
    ),
    w_county_names as (
        select
            cfip.county_name as county,
            iso.*
        from iso
        left join data_warehouse.county_fips as cfip
            on iso.county_id_fips = cfip.county_id_fips
    ),
    w_names as (
        SELECT
            sfip.state_name as state,
            iso.*
        from w_county_names as iso
        left join data_warehouse.state_fips as sfip
            on iso.state_id_fips = sfip.state_id_fips
    )
    select
        iso.*,
        ncsl.permitting_type as state_permitting_type
    from w_names as iso
    left join data_warehouse.ncsl_state_permitting as ncsl
        on iso.state_id_fips = ncsl.state_id_fips
    ;
    """
    df = pd.read_sql(query, engine)
    _estimate_proposed_power_co2e(df)
    return df


def _convert_long_to_wide(long_format: pd.DataFrame) -> pd.DataFrame:
    long = long_format.copy()
    # separate generation from storage
    is_storage = long.loc[:, "resource_clean"].str.contains("storage", flags=IGNORECASE)
    long["storage_type"] = long.loc[:, "resource_clean"].where(is_storage)
    long["generation_type"] = long.loc[:, "resource_clean"].where(~is_storage)
    gen = long.loc[~is_storage, :]
    storage = long.loc[is_storage, :]

    group = gen.groupby("project_id")[["generation_type", "capacity_mw"]]
    # first generation source
    rename_dict = {
        "generation_type": "generation_type_1",
        "capacity_mw": "generation_capacity_mw_1",
    }
    gen_1 = group.nth(0).rename(columns=rename_dict)
    # second generation source (very few rows)
    rename_dict = {
        "generation_type": "generation_type_2",
        "capacity_mw": "generation_capacity_mw_2",
    }
    gen_2 = group.nth(1).rename(columns=rename_dict)
    # shouldn't be any with 3 generation types
    assert group.nth(2).shape[0] == 0
    gen = pd.concat([gen_1, gen_2], axis=1, copy=False)

    # storage
    assert storage.duplicated("project_id").sum() == 0  # no multi-storage projects
    storage = storage.set_index("project_id")[["capacity_mw"]].rename(
        columns={"capacity_mw": "storage_capacity_mw"}
    )

    # combine
    gen_stor = gen.join(storage, how="outer")
    assert (
        len(gen_stor) == long.loc[:, "project_id"].nunique()
    )  # all projects accounted for and 1:1
    co2e = long.groupby("project_id")["co2e_tonnes_per_year"].sum()
    other_cols = (
        long.drop(
            columns=[
                "generation_type",
                "capacity_mw",
                "resource_clean",
                "co2e_tonnes_per_year",
            ]
        )
        .groupby("project_id")
        .nth(0)
    )
    wide = pd.concat([gen_stor, other_cols, co2e], axis=1, copy=False)
    wide.sort_index(inplace=True)
    wide.reset_index(inplace=True)
    wide_col_order = [
        "project_id",
        "project_name",
        "iso_region",
        "entity",
        "utility",
        "developer",
        "state",
        "county",
        "state_id_fips",
        "county_id_fips",
        "resource_class",
        "is_hybrid",
        "generation_type_1",
        "generation_capacity_mw_1",
        "generation_type_2",
        "generation_capacity_mw_2",
        "storage_type",
        "storage_capacity_mw",
        "co2e_tonnes_per_year",
        "date_entered_queue",
        "date_proposed_online",
        "interconnection_status",
        "point_of_interconnection",
        "queue_status",
        "has_ordinance",
        "ordinance_jurisdiction_name",
        "ordinance_jurisdiction_type",
        "ordinance_earliest_year_mentioned",
        "ordinance",
        "state_permitting_type",
    ]
    wide = wide.loc[:, wide_col_order]

    return wide


def _add_derived_columns(mart: pd.DataFrame) -> None:
    mart["has_ordinance"] = mart["ordinance"].notna()
    # This categorizes projects with multiple generation types, but
    # no storage, as 'hybrid'
    mart["is_hybrid"] = mart.duplicated("project_id", keep=False)

    resource_map = {
        "Battery Storage": "storage",
        "Biofuel": "renewable",
        "Biomass": "renewable",
        "Coal": "fossil",
        "Combustion Turbine": "fossil",
        "CSP": "renewable",
        "Fuel Cell": "renewable",
        "Geothermal": "renewable",
        "Hydro": "renewable",
        "Landfill Gas": "fossil",
        "Methane; Solar": "other",
        "Municipal Solid Waste": "fossil",
        "Natural Gas; Other; Storage; Solar": "fossil",
        "Natural Gas; Storage": "fossil",
        "Natural Gas": "fossil",
        "Nuclear": "other",
        "Offshore Wind": "renewable",
        "Oil; Biomass": "fossil",
        "Oil": "fossil",
        "Onshore Wind": "renewable",
        "Other Storage": "storage",
        "Other": "fossil",
        "Pumped Storage": "storage",
        "Solar; Biomass": "renewable",
        "Solar; Storage": "renewable",
        "Solar": "renewable",
        "Steam": np.nan,
        "Unknown": np.nan,
        "Waste Heat": "fossil",
        "Wind; Storage": "renewable",
        np.nan: np.nan,
    }
    # note that this classifies pure storage facilities as np.nan
    resources_in_data = set(mart["resource_clean"].unique())
    mapped_resources = set(resource_map.keys())
    not_mapped = resources_in_data.difference(mapped_resources)
    assert len(not_mapped) == 0, f"Unmapped resource type(s): {not_mapped}"
    mart["resource_class"] = mart["resource_clean"].map(resource_map)

    return mart


def create_long_format(engine: sa.engine.Engine) -> pd.DataFrame:
    iso = _get_and_join_iso_tables(engine)
    all_counties = _get_county_fips_df(engine)
    all_states = _get_state_fips_df(engine)

    # model local opposition
    aggregator = CountyOpposition(
        engine=engine, county_fips_df=all_counties, state_fips_df=all_states
    )
    combined_opp = aggregator.agg_to_counties(include_state_policies=True)
    rename_dict = {
        "geocoded_locality_name": "ordinance_jurisdiction_name",
        "geocoded_locality_type": "ordinance_jurisdiction_type",
        "earliest_year_mentioned": "ordinance_earliest_year_mentioned",
    }
    combined_opp.rename(columns=rename_dict, inplace=True)

    long_format = iso.merge(
        combined_opp, on="county_id_fips", how="left", validate="m:1"
    )
    _add_derived_columns(long_format)
    return long_format


def create_data_mart(engine: Optional[sa.engine.Engine] = None) -> pd.DataFrame:
    """Create projects datamart dataframe."""
    if engine is None:
        engine = get_sql_engine()

    long_format = create_long_format(engine)
    wide_format = _convert_long_to_wide(long_format)

    return {
        "iso_projects_long_format": long_format,
        "iso_projects_wide_format": wide_format,
    }
