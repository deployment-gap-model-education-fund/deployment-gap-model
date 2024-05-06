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


def _get_gridstatus_projects(engine: sa.engine.Engine) -> pd.DataFrame:
    # drops transmission projects
    query = """
    WITH
    proj_res AS (
        SELECT
            queue_id,
            is_nearly_certain,
            project_id,
            project_name,
            capacity_mw,
            developer,
            entity,
            entity AS iso_region, -- these are different in non-ISO data from LBNL
            utility,
            proposed_completion_date AS date_proposed_online,
            point_of_interconnection,
            is_actionable,
            resource_clean,
            queue_status,
            queue_date AS date_entered_queue,
            interconnection_status_raw AS interconnection_status
        FROM data_warehouse.gridstatus_projects as proj
        LEFT JOIN data_warehouse.gridstatus_resource_capacity as res
        USING (project_id)
        WHERE resource_clean != 'Transmission'
    ),
    loc as (
        -- projects can have multiple locations, though 99 percent have only one.
        -- Can multiply capacity by frac_locations_in_county to allocate it equally.
        SELECT
            project_id,
            state_id_fips,
            county_id_fips,
            (1.0 / count(*) over (partition by project_id))::real as frac_locations_in_county
        FROM data_warehouse.gridstatus_locations
    ),
    gs as (
        SELECT
            proj_res.*,
            loc.state_id_fips,
            loc.county_id_fips,
            -- projects with missing location info get full capacity allocation
            coalesce(loc.frac_locations_in_county, 1.0) as frac_locations_in_county
        FROM proj_res
        LEFT JOIN loc
        USING (project_id)
    )
    SELECT
        sfip.state_name AS state,
        cfip.county_name AS county,
        gs.*,
        'gridstatus' AS source,
        ncsl.permitting_type AS state_permitting_type
    FROM gs
    LEFT JOIN data_warehouse.ncsl_state_permitting AS ncsl
        on gs.state_id_fips = ncsl.state_id_fips
    LEFT JOIN data_warehouse.state_fips AS sfip
        ON gs.state_id_fips = sfip.state_id_fips
    LEFT JOIN data_warehouse.county_fips AS cfip
        ON gs.county_id_fips = cfip.county_id_fips
    """
    gs = pd.read_sql(query, engine)
    return gs


def _merge_lbnl_with_gridstatus(lbnl: pd.DataFrame, gs: pd.DataFrame) -> pd.DataFrame:
    """Merge non ISO LBNL projects with ISO projects in GridStatus.

    Args:
        lbnl: lbnl ISO queue projects
        engine: engine to connect to the local postgres data warehouse
    """
    is_non_iso = lbnl.iso_region.str.contains("non-ISO")
    lbnl_non_isos = lbnl.loc[is_non_iso, :].copy()

    # TODO (bendnorman): How should we handle project_ids? This hack
    # isn't ideal because the GS data warehouse and data mart project
    # ids aren't consistent
    max_lbnl_id = lbnl_non_isos.project_id.max() + 1
    gs["project_id"] = list(range(max_lbnl_id, max_lbnl_id + len(gs)))

    shared_ids = set(gs.project_id).intersection(set(lbnl_non_isos.project_id))
    assert len(shared_ids) == 0, f"Found duplicate ids between GS and LBNL {shared_ids}"

    fields_in_gs_not_in_lbnl = gs.columns.difference(lbnl.columns)
    fields_in_lbnl_not_in_gs = lbnl.columns.difference(gs.columns)
    assert (
        fields_in_gs_not_in_lbnl.empty
    ), f"These columns are in Grid Status but not LBNL: {fields_in_gs_not_in_lbnl}"
    assert (
        fields_in_lbnl_not_in_gs.empty
    ), f"These columns are in LBNL but not Grid Status: {fields_in_lbnl_not_in_gs}"

    return pd.concat([gs, lbnl_non_isos], axis=0, ignore_index=True)


def _get_lbnl_projects(engine: sa.engine.Engine, non_iso_only=True) -> pd.DataFrame:
    where_clause = "WHERE region ~ 'non-ISO'" if non_iso_only else ""
    query = f"""
    WITH
    iso_proj_res as (
        SELECT
            proj.project_id,
            proj.queue_id,
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
            proj.is_actionable,
            proj.is_nearly_certain,
            res.capacity_mw,
            res.resource_clean
        FROM data_warehouse.iso_projects as proj
        INNER JOIN data_warehouse.iso_resource_capacity as res
        ON proj.project_id = res.project_id
        {where_clause}
    ),
    loc as (
        -- Remember that projects can have multiple locations, though 99 percent have only one.
        -- Can optionally multiply capacity by frac_locations_in_county to allocate it equally.
        SELECT
            project_id,
            state_id_fips,
            county_id_fips,
            (1.0 / count(*) over (partition by project_id))::real as frac_locations_in_county
        FROM data_warehouse.iso_locations
    ),
    iso as (
        SELECT
            iso_proj_res.*,
            loc.state_id_fips,
            loc.county_id_fips,
            -- projects with missing location info get full capacity allocation
            coalesce(loc.frac_locations_in_county, 1.0) as frac_locations_in_county
        from iso_proj_res
        LEFT JOIN loc
        ON iso_proj_res.project_id = loc.project_id
    )
    SELECT
        sfip.state_name as state,
        cfip.county_name as county,
        iso.*,
        'lbnl' as source,
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
    # one whole-row duplicate due to a multi-county project with missing state value.
    # Makes both county_id_fips and state_id_fips null.
    dupes = df.duplicated(keep="first")
    assert dupes.sum() == 0, f"Expected 0 duplicates, found {dupes.sum()}."
    return df


def _get_and_join_iso_tables(
    engine: sa.engine.Engine, use_gridstatus=True, use_proprietary_offshore=True
) -> pd.DataFrame:
    """Get ISO projects.

    PK should be (project_id, county_id_fips, resource_clean), but county_id_fips has nulls.

    Note that this duplicates projects that have multiple prospective locations. Use the frac_locations_in_county
    column to allocate capacity and co2e estimates to counties when aggregating.
    Otherwise they will be double-counted.

    Args:
        engine: engine to connect to the local postgres data warehouse
        use_gridstatus: use gridstatus data for ISO projects.

    Returns:
        A dataframe of ISO projects with location, capacity, estimated co2 emissions and state permitting info.
    """
    if use_gridstatus:
        lbnl = _get_lbnl_projects(engine, non_iso_only=True)
        gs = _get_gridstatus_projects(engine)
        out = _merge_lbnl_with_gridstatus(lbnl=lbnl, gs=gs)
    else:
        out = _get_lbnl_projects(engine, non_iso_only=False)
    if use_proprietary_offshore:
        offshore = _get_proprietary_proposed_offshore(engine)
        out = _replace_iso_offshore_with_proprietary(out, offshore)
    _estimate_proposed_power_co2e(out)
    return out


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
        assoc.project_id,
        assoc.county_id_fips,
        -- projects with missing location info get full capacity allocation
        CASE WHEN assoc.frac_locations_in_county IS NULL
            THEN 1.0
            ELSE assoc.frac_locations_in_county
            END as frac_locations_in_county,
        substr(assoc.county_id_fips, 1, 2) as state_id_fips,

        proj.name as project_name,
        proj.developer,
        proj."capacity_mw",
        date(proj.proposed_completion_year::text || '-01-01') as date_proposed_online,
        'active' as queue_status,
        'Offshore Wind' as resource_clean,
        0.0 as co2e_tonnes_per_year,
        proj.is_actionable,
        proj.is_nearly_certain,
        'proprietary' as source,

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
        [iso_to_keep, proprietary],
        axis=0,
        ignore_index=True,
    )
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
        "is_actionable",
        "is_nearly_certain",
        "source",
        # "frac_locations_in_county", not needed in wide format
    ]
    wide = wide.loc[:, wide_col_order]

    return wide


def _add_derived_columns(mart: pd.DataFrame) -> None:
    mart["ordinance_via_reldi"] = mart["ordinance_text"].notna()
    priority_ban = mart["ordinance_via_self_maintained"]
    secondary_ban_cols = [
        "ordinance_via_reldi",
        "ordinance_via_solar_nrel",
        "ordinance_via_wind_nrel",
    ]
    mart["ordinance_is_restrictive"] = priority_ban.fillna(
        mart[secondary_ban_cols].fillna(False).any(axis=1)
    )
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
        "Transmission": "transmission",
        "Unknown": np.nan,
        "Waste Heat": "fossil",
        "Wind; Storage": "renewable",
        np.nan: np.nan,  # not technically necessary but make it explicit
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
    iso = _get_and_join_iso_tables(
        engine, use_gridstatus=True, use_proprietary_offshore=True
    )
    all_counties = _get_county_fips_df(engine)
    all_states = _get_state_fips_df(engine)

    # model local opposition
    aggregator = CountyOpposition(
        engine=engine, county_fips_df=all_counties, state_fips_df=all_states
    )
    combined_opp = aggregator.agg_to_counties(
        include_state_policies=False,
        include_nrel_bans=True,
        include_manual_ordinances=True,
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


def get_eia860m_current(
    engine: sa.engine.Engine, date_as_of: Optional[str] = None
) -> pd.DataFrame:
    """Get the most recent EIA860M data."""
    if not date_as_of:  # get most recent data
        date_as_of = (
            pd.read_sql(
                "SELECT max(valid_until_date) FROM data_warehouse.pudl_eia860m_changelog",
                engine,
            )
            .iat[0, 0]
            .strftime("%Y-%m-%d")
        )
    else:
        raise NotImplementedError(
            "Getting data as of a specific date is not yet implemented."
        )
    query = f"""
    SELECT
        report_date,
        plant_id_eia,
        plant_name_eia,
        utility_id_eia,
        utility_name_eia,
        generator_id,
        capacity_mw,
        state,
        county,
        current_planned_generator_operating_date,
        -- data_maturity,
        energy_source_code_1,
        prime_mover_code,
        energy_storage_capacity_mwh,
        fuel_type_code_pudl,
        generator_retirement_date,
        latitude,
        longitude,
        -- net_capacity_mwdc,
        operational_status_code,
        operational_status AS operational_status_category,
        planned_derate_date,
        planned_generator_retirement_date,
        planned_net_summer_capacity_derate_mw,
        planned_net_summer_capacity_uprate_mw,
        planned_uprate_date,
        technology_description,
        -- summer_capacity_mw,
        -- winter_capacity_mw,
        -- valid_until_date
        state_id_fips,
        county_id_fips
    FROM data_warehouse.pudl_eia860m_changelog
    WHERE valid_until_date = '{date_as_of}'
    ORDER BY plant_id_eia, generator_id
    """
    current_projects = pd.read_sql(query, engine)
    return current_projects


def get_eia860m_status_history(
    engine: sa.engine.Engine, end_date: Optional[str] = None
) -> pd.DataFrame:
    """Get the EIA860M status for each project for each of the past 12 quarters."""
    if end_date:
        raise NotImplementedError(
            "Getting data as of a specific date is not yet implemented."
        )
    query = """
    WITH
    quarterly_long_format as (
        SELECT
            plant_name_eia,
            plant_id_eia,
            generator_id,
            CASE
                -- I represent quarters via the first day of the last month of each quarter.
                -- The last month because I want to use quarter-end to match the convention
                -- used elsewhere in this repo (eg. the ISO changelog).
                -- The first day because 860m is released monthly and PUDL convention is to
                -- represent months via the date of the first of the month.
                WHEN timestamp '2023-12-01' >= report_date AND (timestamp '2023-12-01' <= valid_until_date OR valid_until_date IS NULL) THEN timestamp '2023-12-01'
                WHEN timestamp '2023-09-01' >= report_date AND (timestamp '2023-09-01' <= valid_until_date OR valid_until_date IS NULL) THEN timestamp '2023-09-01'
                WHEN timestamp '2023-06-01' >= report_date AND (timestamp '2023-06-01' <= valid_until_date OR valid_until_date IS NULL) THEN timestamp '2023-06-01'
                WHEN timestamp '2023-03-01' >= report_date AND (timestamp '2023-03-01' <= valid_until_date OR valid_until_date IS NULL) THEN timestamp '2023-03-01'
                WHEN timestamp '2022-12-01' >= report_date AND (timestamp '2022-12-01' <= valid_until_date OR valid_until_date IS NULL) THEN timestamp '2022-12-01'
                WHEN timestamp '2022-09-01' >= report_date AND (timestamp '2022-09-01' <= valid_until_date OR valid_until_date IS NULL) THEN timestamp '2022-09-01'
                WHEN timestamp '2022-06-01' >= report_date AND (timestamp '2022-06-01' <= valid_until_date OR valid_until_date IS NULL) THEN timestamp '2022-06-01'
                WHEN timestamp '2022-03-01' >= report_date AND (timestamp '2022-03-01' <= valid_until_date OR valid_until_date IS NULL) THEN timestamp '2022-03-01'
                WHEN timestamp '2021-12-01' >= report_date AND (timestamp '2021-12-01' <= valid_until_date OR valid_until_date IS NULL) THEN timestamp '2021-12-01'
                WHEN timestamp '2021-09-01' >= report_date AND (timestamp '2021-09-01' <= valid_until_date OR valid_until_date IS NULL) THEN timestamp '2021-09-01'
                WHEN timestamp '2021-06-01' >= report_date AND (timestamp '2021-06-01' <= valid_until_date OR valid_until_date IS NULL) THEN timestamp '2021-06-01'
                WHEN timestamp '2021-03-01' >= report_date AND (timestamp '2021-03-01' <= valid_until_date OR valid_until_date IS NULL) THEN timestamp '2021-03-01'
                ELSE NULL
            END AS status_date,
            operational_status_code
        FROM data_warehouse.pudl_eia860m_changelog
    )
    select
        *
    from quarterly_long_format
    where status_date is not null
    order by 2,3,4 desc
    """
    status_history = pd.read_sql(query, engine)
    # reshape to wide format
    status_history = status_history.pivot(
        index=["plant_id_eia", "generator_id"],
        columns="status_date",
        values="operational_status_code",
    )
    status_history.columns = [
        "status" + d for d in status_history.columns.strftime("%Y-%m-%d")
    ]
    return status_history


def _get_eia860m_transition_dates(engine: sa.engine.Engine) -> pd.DataFrame:
    """Get the dates of status transitions for each project."""
    query = """
    SELECT
        plant_id_eia,
        generator_id,
        COALESCE(operational_status_code::text, operational_status) as operational_status_code,
        min(report_date) as status_date
    FROM data_warehouse.pudl_eia860m_changelog
    group by 1,2,3
    order by 1,2,3
    """
    transition_dates = pd.read_sql(query, engine)
    # reshape to wide format
    transition_dates = transition_dates.pivot(
        index=["plant_id_eia", "generator_id"],
        columns="operational_status_code",
        values="status_date",
    )
    transition_dates.add_prefix("date_entered_", axis=1, inplace=True)
    return transition_dates


def create_data_mart(
    engine: Optional[sa.engine.Engine] = None,
) -> dict[str, pd.DataFrame]:
    """Create projects datamart dataframe."""
    if engine is None:
        engine = get_sql_engine()

    # long_format = create_long_format(engine)
    # wide_format = _convert_long_to_wide(long_format)

    eia860m_current = get_eia860m_current(engine)
    eia860m_history = get_eia860m_status_history(engine)
    eia860m_transition_dates = _get_eia860m_transition_dates(engine)

    return {
        # "iso_projects_long_format": long_format,
        # "iso_projects_wide_format": wide_format,
        "projects_current_860m": eia860m_current,
        "projects_history_860m": eia860m_history,
        "projects_transition_dates_860m": eia860m_transition_dates,
    }


if __name__ == "__main__":
    # debugging entry point
    mart = create_data_mart()
    print("yeehaw")
