"""Module to create a project-level table for DBCP to use in spreadsheet tools."""
from re import IGNORECASE
from typing import Optional, Sequence

import numpy as np
import pandas as pd
import sqlalchemy as sa

from dbcp.data_mart.helpers import _estimate_proposed_power_co2e
from dbcp.helpers import get_sql_engine


def _subset_db_columns(
    columns: Sequence[str], table: str, engine: sa.engine.Engine
) -> pd.DataFrame:
    query = f"SELECT {', '.join(columns)} FROM {table}"
    df = pd.read_sql(query, engine)
    return df


def _get_and_join_iso_tables(engine: sa.engine.Engine) -> pd.DataFrame:
    query = """
    WITH
    proj_res as (
        SELECT
            proj.project_id,
            proj.date_operational,
            proj.date_proposed as date_proposed_online,
            proj.date_withdrawn,
            proj.days_in_queue,
            proj.developer,
            proj.entity,
            proj.interconnection_status_lbnl as interconnection_status,
            proj.point_of_interconnection,
            proj.project_name,
            proj.queue_date as date_entered_queue,
            proj.queue_status,
            proj.region as iso_region,
            proj.utility,
            proj.withdrawl_reason,
            res.capacity_mw,
            res.resource_clean
        FROM data_warehouse.iso_projects as proj
        INNER JOIN data_warehouse.iso_resource_capacity as res
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
        FROM data_warehouse.iso_locations
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


def _get_local_opposition_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        # 'geocoded_containing_county',  # only need FIPS, names come from elsewhere
        "county_id_fips",
        "earliest_year_mentioned",
        # 'latest_year_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
        "geocoded_locality_name",
        "geocoded_locality_type",
        # 'n_years_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
        "ordinance",
        # 'raw_locality_name',  # drop raw name in favor of canonical one
        # 'raw_state_name',  # drop raw name in favor of canonical one
        # 'state_id_fips',  # will join on 5-digit county FIPS, which includes state
    ]
    db = "data_warehouse.local_ordinance"
    df = _subset_db_columns(cols, db, engine)
    return df


def _get_state_opposition_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = [
        "earliest_year_mentioned",
        # 'latest_year_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
        # 'n_years_mentioned',  # for simplicity, only include one year metric (earliest_year_mentioned)
        "policy",
        # 'raw_state_name',  # drop raw name in favor of canonical one
        "state_id_fips",
    ]
    db = "data_warehouse.state_policy"
    df = _subset_db_columns(cols, db, engine)
    return df


def _get_county_fips_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = ["*"]
    db = "data_warehouse.county_fips"
    df = _subset_db_columns(cols, db, engine)
    return df


def _get_state_fips_df(engine: sa.engine.Engine) -> pd.DataFrame:
    cols = ["*"]
    db = "data_warehouse.state_fips"
    df = _subset_db_columns(cols, db, engine)
    return df


def _filter_state_opposition(state_df: pd.DataFrame) -> pd.DataFrame:
    """Drop states that repealed their policies or whose policy was pro-renewables instead of anti-renewables.

    Args:
        state_df (pd.DataFrame): state policy dataframe

    Returns:
        pd.DataFrame: filtered copy of the input state policy dataframe
    """
    fips_codes_to_drop = {"23", "36"}  # Maine (repealed), New York (pro-RE)
    not_dropped = ~state_df.loc[:, "state_id_fips"].isin(fips_codes_to_drop)
    filtered_state_df = state_df.loc[not_dropped, :].copy()
    return filtered_state_df


def _represent_state_opposition_as_counties(
    state_df: pd.DataFrame, county_fips_df: pd.DataFrame, state_fips_df: pd.DataFrame
) -> pd.DataFrame:
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
    states_as_counties = state_df.merge(
        county_fips_df.loc[:, ["county_id_fips", "state_id_fips"]],
        on="state_id_fips",
        how="left",
    )

    # replicate local opposition columns
    # geocoded_locality_name
    states_as_counties = states_as_counties.merge(
        state_fips_df.loc[:, ["state_name", "state_id_fips"]],
        on="state_id_fips",
        how="left",
    )
    # geocoded_locality_type
    states_as_counties["geocoded_locality_type"] = "state"
    rename_dict = {
        "state_name": "geocoded_locality_name",
        "policy": "ordinance",
    }
    states_as_counties = states_as_counties.rename(columns=rename_dict).drop(
        columns=["state_id_fips"]
    )
    return states_as_counties


def _agg_local_ordinances_to_counties(ordinances: pd.DataFrame) -> pd.DataFrame:
    r"""Force the local ordinance table to have 1 row = 1 county. Only 8/92 counties have multiple ordinances.

    This is necessary for joining into the ISO project table. ISO projects are only located by county.
    Aggregation method:
    * take min of earliest_year_mentioned
    * if only one geocoded_locality_name, use it. Otherwise replace with "multiple"
    * same with 'locality type'
    * concatenate the rdinances, each with geocoded_locality_name prefix, eg "Great County: <ordinance>\nSmall Town: <ordinance>"

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
    dupe_counties = ordinances.duplicated(subset="county_id_fips", keep=False)
    dupes = ordinances.loc[dupe_counties, :].copy()
    not_dupes = ordinances.loc[~dupe_counties, :].copy()

    dupes["ordinance"] = (
        dupes["geocoded_locality_name"] + ": " + dupes["ordinance"] + "\n"
    )
    grp = dupes.groupby("county_id_fips")

    years = grp["earliest_year_mentioned"].min()

    n_unique = grp[["geocoded_locality_name", "geocoded_locality_type"]].nunique()
    localities = (
        grp[["geocoded_locality_name", "geocoded_locality_type"]]
        .nth(0)
        .mask(n_unique > 1, other="multiple")
    )

    descriptions = grp["ordinance"].sum().str.strip()

    agg_dupes = pd.concat([years, localities, descriptions], axis=1).reset_index()
    recombined = pd.concat(
        [not_dupes, agg_dupes], axis=0, ignore_index=True
    ).sort_values("county_id_fips")
    assert not recombined.duplicated(subset="county_id_fips").any()

    return recombined


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
    return wide


def _add_derived_columns(mart: pd.DataFrame) -> None:
    mart["has_ordinance"] = mart["ordinance"].notna()
    # This categorizes projects with multiple generation types, but
    # no storage, as 'hybrid'
    mart["is_hybrid"] = mart.duplicated("project_id", keep=False)

    resource_map = {
        "Battery Storage": "storage",
        "Pumped Storage": "storage",
        "Other Storage": "storage",
        "Solar; Storage": "renewable",
        "Wind; Storage": "renewable",
        "Natural Gas; Other; Storage; Solar": "fossil",
        "Natural Gas; Storage": "fossil",
        "Onshore Wind": "renewable",
        "Solar": "renewable",
        "Natural Gas": "fossil",
        "Other": "fossil",
        "Hydro": "renewable",
        "Geothermal": "renewable",
        "Offshore Wind": "renewable",
        "Nuclear": "other",
        "Coal": "fossil",
        "Waste Heat": "fossil",
        "Biofuel": "renewable",
        "Biomass": "renewable",
        "Landfill Gas": "fossil",
        "Oil": "fossil",
        "Unknown": np.nan,
        "Combustion Turbine": "fossil",
        "Oil; Biomass": "fossil",
        "Municipal Solid Waste": "fossil",
        "Fuel Cell": "other",
        "Steam": np.nan,
        "Solar; Biomass": "renewable",
        "Methane; Solar": "other",
    }
    # note that this classifies pure storage facilities as np.nan
    assert set(mart["resource_clean"].unique()) == set(resource_map.keys())
    mart["resource_class"] = mart.loc[:, "resource_clean"].map(resource_map)
    return


def create_data_mart(engine: Optional[sa.engine.Engine] = None) -> pd.DataFrame:
    """Create projects datamart dataframe."""
    if engine is None:
        engine = get_sql_engine()
    iso = _get_and_join_iso_tables(engine)
    local_opp = _get_local_opposition_df(engine)
    state_opp = _get_state_opposition_df(engine)
    all_counties = _get_county_fips_df(engine)
    all_states = _get_state_fips_df(engine)

    # model local opposition
    filtered_state_opp = _filter_state_opposition(state_opp)
    states_to_counties = _represent_state_opposition_as_counties(
        filtered_state_opp, county_fips_df=all_counties, state_fips_df=all_states
    )
    combined_opp = pd.concat([local_opp, states_to_counties], axis=0)
    combined_opp = _agg_local_ordinances_to_counties(combined_opp)
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
        "date_operational",
        "date_proposed_online",
        "date_withdrawn",
        "days_in_queue",
        "interconnection_status",
        "point_of_interconnection",
        "queue_status",
        "withdrawl_reason",
        "has_ordinance",
        "ordinance_jurisdiction_name",
        "ordinance_jurisdiction_type",
        "ordinance_earliest_year_mentioned",
        "ordinance",
        "state_permitting_type",
    ]
    wide_format = _convert_long_to_wide(long_format).loc[:, wide_col_order]

    return {
        "iso_projects_long_format": long_format,
        "iso_projects_wide_format": wide_format,
    }
