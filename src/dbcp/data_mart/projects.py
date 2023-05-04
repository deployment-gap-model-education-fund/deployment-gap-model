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
    """Get ISO projects.

    PK should be (project_id, county_id_fips, resource_clean), but county_id_fips has nulls.

    Note that this duplicates projects that have multiple prospective locations. Use the frac_locations_in_county
    column to allocate capacity and co2e estimates to counties when aggregating.
    Otherwise they will be double-counted.
    """
    query = """
    WITH
    iso_proj_res as (
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
        -- Remember that projects can have multiple locations, though 99 percent have only one.
        -- Can optionally multiply capacity by frac_locations_in_county to allocate it equally.
        SELECT
            project_id,
            state_id_fips,
            county_id_fips,
            (1.0 / count(*) over (partition by project_id))::real as frac_locations_in_county
        FROM data_warehouse.iso_locations_2021
    ),
    iso as (
        SELECT
            iso_proj_res.*,
            loc.state_id_fips,
            loc.county_id_fips,
            loc.frac_locations_in_county
        from iso_proj_res
        LEFT JOIN loc
        ON iso_proj_res.project_id = loc.project_id
    )
    SELECT
        sfip.state_name as state,
        cfip.county_name as county,
        iso.*,
        ncsl.permitting_type as state_permitting_type
    from iso
    left join data_warehouse.state_fips as sfip
        on iso.state_id_fips = sfip.state_id_fips
    left join data_warehouse.county_fips as cfip
        on iso.county_id_fips = cfip.county_id_fips
    left join data_warehouse.ncsl_state_permitting as ncsl
        on iso.state_id_fips = ncsl.state_id_fips
    ;
    """
    df = pd.read_sql(query, engine)
    # projects with missing location info get full capacity allocation
    df["frac_locations_in_county"].fillna(1.0, inplace=True)
    # two whole-row dupes due to both town and county names present in raw data
    dupes = df.duplicated(keep="first")
    assert df.loc[dupes, "county"].isin({"Wilbarger", None}).all()
    assert dupes.sum() == 2, f"Expected 2 duplicate rows, got {dupes.sum()}."
    df = df.loc[~dupes]
    _estimate_proposed_power_co2e(df)
    return df


def _get_proprietary_proposed_offshore(engine: sa.engine.Engine) -> pd.DataFrame:
    """Get proprietary offshore wind data in a format that imitates the ISO queues.

    PK is (project_id, county_id_fips).

    Note that this duplicates projects that have multiple cable landings. Use the frac_locations_in_county
    column to allocate capacity and co2e estimates to counties when aggregating.
    Otherwise they will be double-counted.
    """
    query = """
    WITH
    cable_locs as (
        SELECT
            project_id,
            locs.county_id_fips,
            COUNT(*) OVER(PARTITION BY project_id) AS n_locations
        FROM data_warehouse.offshore_wind_cable_landing_association as cable
        INNER JOIN data_warehouse.offshore_wind_locations as locs
        USING(location_id)
    ),
    proj_county_assoc as (
        SELECT
            project_id,
            county_id_fips,
            -- some counties have multiple cable landings from the same
            -- project (different towns). I allocate the capacity equally
            -- over the landings
            (count(*) * 1.0 / max(n_locations))::real as frac_locations_in_county
        FROM cable_locs
        group by 1,2
    )
    -- join the project, state, and county stuff
    SELECT
        assoc.*,
        substr(assoc.county_id_fips, 1, 2) as state_id_fips,

        proj.name as project_name,
        proj.developer,
        proj."capacity_mw",
        date(proj.proposed_completion_year::text || '-01-01') as date_proposed_online,
        'active' as queue_status,
        'Offshore Wind' as resource_clean,
        0.0 as co2e_tonnes_per_year,

        sfip.state_name as state,
        cfip.county_name as county,
        ncsl.permitting_type as state_permitting_type

    FROM proj_county_assoc as assoc
    INNER JOIN data_warehouse.offshore_wind_projects as proj
    USING(project_id)
    LEFT JOIN data_warehouse.state_fips as sfip
    ON substr(assoc.county_id_fips, 1, 2) = sfip.state_id_fips
    LEFT JOIN data_warehouse.county_fips as cfip
    USING(county_id_fips)
    LEFT JOIN data_warehouse.ncsl_state_permitting as ncsl
    ON substr(assoc.county_id_fips, 1, 2) = ncsl.state_id_fips
    WHERE proj.construction_status != 'Online'
    ;
    """
    df = pd.read_sql(query, engine)
    return df


def _replace_iso_offshore_with_proprietary(
    iso_queues: pd.DataFrame, proprietary: pd.DataFrame
) -> pd.DataFrame:
    """Replace offshore wind projects in the ISO queues with proprietary data.

    PK should be (source, project_id, county_id_fips, resource_clean), but county_id_fips has nulls.
    """
    iso_to_keep = iso_queues.loc[iso_queues["resource_clean"] != "Offshore Wind", :]
    out = pd.concat(
        [iso_to_keep, proprietary.assign(source="proprietary")],
        axis=0,
        ignore_index=True,
    )
    out["source"].fillna("iso", inplace=True)
    return out


def _convert_long_to_wide(long_format: pd.DataFrame) -> pd.DataFrame:
    """Restructure the long-format data as a single row per project.

    PK is (source, project_id)
    1:m relationships are handled by creating multiple columns for each m.
    Wide format is ugly but it's what the people want.
    """
    long = long_format.copy()
    # separate generation from storage
    is_storage = long.loc[:, "resource_clean"].str.contains("storage", flags=IGNORECASE)
    long["storage_type"] = long.loc[:, "resource_clean"].where(is_storage)
    long["generation_type"] = long.loc[:, "resource_clean"].where(~is_storage)
    gen = long.loc[~is_storage, :]
    storage = long.loc[is_storage, :]

    group_keys = ["project_id", "source", "county_id_fips"]
    # create multiple generation columns
    group = gen.groupby(group_keys, dropna=False)[["generation_type", "capacity_mw"]]
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

    # create storage column
    assert storage.duplicated(subset=group_keys).sum() == 0  # no multi-storage projects
    storage = storage.set_index(group_keys)[["capacity_mw"]].rename(
        columns={"capacity_mw": "storage_capacity_mw"}
    )

    # combine gen and storage cols
    gen_stor = gen.join(storage, how="outer")
    assert (
        len(gen_stor) == long.groupby(group_keys, dropna=False).ngroups
    )  # all project-locations accounted for and 1:1
    co2e = long.groupby(group_keys, dropna=False)["co2e_tonnes_per_year"].sum()
    other_cols = (
        long.drop(
            columns=[
                "generation_type",
                "capacity_mw",
                "resource_clean",
                "co2e_tonnes_per_year",
            ]
        )
        .groupby(group_keys, dropna=False)
        .nth(0)
    )
    project_locations = pd.concat([gen_stor, other_cols, co2e], axis=1, copy=False)

    # now create multiple location columns
    project_keys = ["source", "project_id"]
    projects = project_locations.reset_index("county_id_fips").groupby(
        project_keys, dropna=False
    )
    loc1 = projects.nth(0).rename(
        columns={"county_id_fips": "county_id_fips_1", "county": "county_1"}
    )
    assert (
        not loc1.index.to_frame().isna().any().any()
    ), "Nulls found in project_id or source."
    loc2 = (
        projects[["county_id_fips", "county"]]
        .nth(1)
        .rename(columns={"county_id_fips": "county_id_fips_2", "county": "county_2"})
    )
    assert projects.nth(2).shape[0] == 0, "More than 2 locations found for a project."

    wide = pd.concat([loc1, loc2], axis=1, copy=False)
    wide.sort_index(inplace=True)
    wide.reset_index(inplace=True)
    wide.rename(
        columns={"state": "state_1", "state_id_fips": "state_id_fips_1"}, inplace=True
    )
    wide_col_order = [
        "project_id",
        "project_name",
        "iso_region",
        "entity",
        "utility",
        "developer",
        "state_1",
        "state_id_fips_1",
        "county_1",
        "county_id_fips_1",
        "county_2",
        "county_id_fips_2",
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
        "ordinance_via_reldi",
        "ordinance_jurisdiction_name",
        "ordinance_jurisdiction_type",
        "ordinance_earliest_year_mentioned",
        "ordinance_text",
        "state_permitting_type",
        "source",
        # "frac_locations_in_county", not needed in wide format
    ]
    wide = wide.loc[:, wide_col_order]

    return wide


def _add_derived_columns(mart: pd.DataFrame) -> None:
    mart["ordinance_via_reldi"] = mart["ordinance_text"].notna()
    ban_cols = [
        "ordinance_via_reldi",
        "ordinance_via_solar_nrel",
        "ordinance_via_wind_nrel",
    ]
    mart["ordinance_is_restrictive"] = mart[ban_cols].fillna(False).any(axis=1)
    # This categorizes any project with multiple generation or storage types as 'hybrid'
    mart["is_hybrid"] = (
        mart.groupby(["source", "project_id", "county_id_fips"])["resource_clean"]
        .transform("size")
        .gt(1)
    )

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

    return


def create_long_format(engine: sa.engine.Engine) -> pd.DataFrame:
    """Create table of ISO projects in long format.

    PK should be (source, project_id, county_id_fips, resource_clean), but county_id_fips has nulls.
    So I added a surrogate key.

    Note that this duplicates projects with multiple prospective locations. Use the frac_locations_in_county
    column to allocate capacity and co2e estimates to counties when aggregating.
    Otherwise they will be double-counted.

    Args:
        engine (sa.engine.Engine): postgres database engine

    Returns:
        pd.DataFrame: long format table of ISO projects
    """
    iso = _get_and_join_iso_tables(engine)
    offshore = _get_proprietary_proposed_offshore(engine)
    iso = _replace_iso_offshore_with_proprietary(iso, offshore)
    all_counties = _get_county_fips_df(engine)
    all_states = _get_state_fips_df(engine)

    # model local opposition
    aggregator = CountyOpposition(
        engine=engine, county_fips_df=all_counties, state_fips_df=all_states
    )
    combined_opp = aggregator.agg_to_counties(
        include_state_policies=False, include_nrel_bans=True
    )
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
    pk = ["source", "project_id", "county_id_fips", "resource_clean"]
    assert long_format.duplicated(subset=pk).sum() == 0, "Duplicate rows in long format"
    long_format["surrogate_id"] = range(len(long_format))
    return long_format


def create_data_mart(
    engine: Optional[sa.engine.Engine] = None,
) -> dict[str, pd.DataFrame]:
    """Create projects datamart dataframe."""
    if engine is None:
        engine = get_sql_engine()

    long_format = create_long_format(engine)
    wide_format = _convert_long_to_wide(long_format)

    return {
        "iso_projects_long_format": long_format,
        "iso_projects_wide_format": wide_format,
    }
