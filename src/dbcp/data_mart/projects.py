"""Module to create a project-level table for DBCP to use in spreadsheet tools."""
import logging
from datetime import datetime
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

logger = logging.getLogger(__name__)

CHANGE_LOG_REGIONS = ("MISO", "NYISO", "ISONE", "PJM", "CAISO", "SPP")


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
            actual_completion_date,
            withdrawn_date,
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
            proj.actual_completion_date,
            proj.withdrawn_date,
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
    # There are two projects that are missing state values in the raw data.
    dupes = df.duplicated(keep="first")
    expected_dupes = 2
    assert (
        dupes.sum() == expected_dupes
    ), f"Expected {expected_dupes} duplicates, found {dupes.sum()}."
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


def create_long_format(
    engine: sa.engine.Engine, active_projects_only: bool = True
) -> pd.DataFrame:
    """Create table of ISO projects in long format.

    PK should be (source, project_id, county_id_fips, resource_clean), but county_id_fips has nulls.
    So I added a surrogate key.

    Note that this duplicates projects with multiple prospective locations. Use the frac_locations_in_county
    column to allocate capacity and co2e estimates to counties when aggregating.
    Otherwise they will be double-counted.

    Args:
        engine: postgres database engine
        active_projects_only: If we only want active projects, grab active projects and
            remove withdrawn_date and actual_completion_date.

    Returns:
        long format table of ISO projects
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
    expected_dupes = 2
    dupes = long_format.duplicated(subset=pk)
    assert (
        dupes.sum() == expected_dupes
    ), f"Expected {expected_dupes} duplicates, found {dupes.sum()}."
    long_format["surrogate_id"] = range(len(long_format))

    # If we only want active projects, grab active projects and remove withdrawn_date and actual_completion_date
    if active_projects_only:
        active_long_format = long_format.query("queue_status == 'active'")
        # drop actual_completion_date and withdrawn_date columns
        active_long_format = active_long_format.drop(
            columns=["actual_completion_date", "withdrawn_date"]
        )
        return active_long_format
    return long_format


def create_geography_change_log(
    change_log: pd.DataFrame, geography: str = "county_id_fips", freq: str = "Q"
) -> pd.DataFrame:
    """Creates a change log of ISO queue projects by geography.

    Each row is a snap shot of the number of projects and capacity in each status and resource class for a given geography and date.

    Currently only includes regions with high coveraage of operational and withdrawn dates: MISO, NYISO, ISONE, PJM, CAISO, SPP.
    ERCOT will require integrating multiple snapshots of data.
    """
    group_keys = [
        geography,
        pd.Grouper(key="effective_date", freq=freq),
        "queue_status",
        "resource_class",
    ]
    geography_change_log = (
        change_log.groupby(group_keys)
        .agg({"project_id": "count", "capacity_mw": "sum"})
        .reset_index()
        .rename(
            columns={
                "project_id": "n_projects",
                "capacity_mw": "capacity_mw",
                "effective_date": "date",
            }
        )
    )
    geography_change_log = geography_change_log.pivot(
        index=[geography, "date"],
        columns=["queue_status", "resource_class"],
        values=["n_projects", "capacity_mw"],
    )
    geography_change_log = geography_change_log.fillna(0)
    geography_change_log.columns = [
        f"{queue_status}_{resource_class}_{col}"
        for col, queue_status, resource_class in geography_change_log.columns.values
    ]
    geography_change_log = geography_change_log.reset_index()
    # add county and state information to the change log
    if geography == "county_id_fips":
        geography_info = (
            change_log[["county_id_fips", "county", "state_id_fips", "state"]]
            .drop_duplicates()
            .dropna(subset=["county_id_fips"])
        )
        geography_change_log = geography_change_log.merge(
            geography_info, on="county_id_fips", how="left", validate="m:1"
        )
    return geography_change_log


def create_project_change_log(long_format: pd.DataFrame) -> pd.DataFrame:
    """Create a change log of GridStatus projects.

    There is a row for every time the status of a project changes.
    The effective_date column is the date the status changed and the end_date
    column is the date the status ended. The end_date is null for current statuses
    of projects.

    Args:
        long_format: long format of ISO projects
    Returns:
        chng: change log of ISO projects
    """
    original_long_format = long_format.copy()
    # for projcts where resource_clean == "Unknown", set resource_class to "other" instead of nan
    long_format["resource_class"] = long_format.resource_class.mask(
        long_format.resource_clean.eq("Unknown"), "other"
    )

    long_format = long_format[long_format["iso_region"].isin(CHANGE_LOG_REGIONS)]

    # make sure we are missing less than 10% of withdrawn_date
    withdrawn = long_format.query("queue_status == 'withdrawn'")
    expected_missing = 0.1
    assert (
        withdrawn["withdrawn_date"].isna().sum() / len(withdrawn) < expected_missing
    ), f"More than {expected_missing} of withdrawn_date is missing."

    # For operational projects, fill in missing actual_completion_date with date_proposed_online
    operational = long_format.query("queue_status == 'operational'")
    operational["actual_completion_date"] = operational[
        "actual_completion_date"
    ].fillna(operational["date_proposed_online"])
    long_format.loc[operational.index, "actual_completion_date"] = operational[
        "actual_completion_date"
    ]

    # make sure we are missing less than 10% of actual_completion_date
    operational = long_format.query("queue_status == 'operational'")
    expected_missing = 0.1
    assert (
        operational["actual_completion_date"].isna().sum() / len(operational)
        < expected_missing
    ), f"More than {expected_missing} of actual_completion_date is missing."
    # Log the pct of rows in operational where actual_completion_date comes after the current year
    current_year = pd.Timestamp.now().year
    pct_after_current_year = operational["actual_completion_date"].dt.year.gt(
        current_year
    ).sum() / len(operational)
    logger.debug(
        f"{pct_after_current_year:.2%} of operational projects have actual_completion_date after the current year."
    )
    # make sure pct_after_current_year is less than 0.001 of operational projects
    expected_missing = 0.001
    assert (
        pct_after_current_year < expected_missing
    ), f"More than {expected_missing}% of operational projects have actual_completion_date after the current year."

    # map active projects to "new"
    long_format["queue_status"] = long_format["queue_status"].map(
        {
            "active": "new",
            "withdrawn": "withdrawn",
            "operational": "operational",
            "suspended": "new",  # Treat suspended projects as active/new because we don't have date suspended columns
        }
    )

    # Remove projects that are missing relevant date columns
    status_dates = [
        {"status": "withdrawn", "date": "withdrawn_date"},
        {"status": "operational", "date": "actual_completion_date"},
        {"status": "new", "date": "date_entered_queue"},
    ]
    for status_date in status_dates:
        status = status_date["status"]
        date_col = status_date["date"]
        n_projects_before = len(long_format)

        is_long_format_of_status = long_format["queue_status"].eq(status)
        is_long_format_of_status_and_missing_date = (
            is_long_format_of_status & long_format[date_col].isna()
        )
        logger.info(f"Projects with missing {date_col} for {status}")
        n_projects_of_status_by_region = (
            long_format[is_long_format_of_status]
            .groupby("iso_region")
            .count()
            .surrogate_id
        )
        n_projects_missing_date_by_region = (
            long_format[is_long_format_of_status_and_missing_date]
            .groupby("iso_region")
            .count()
            .surrogate_id
        )
        logger.info(
            (n_projects_missing_date_by_region / n_projects_of_status_by_region).fillna(
                0
            )
        )

        # Keep all projects that are not missing the date column
        long_format = long_format[
            ~(is_long_format_of_status & is_long_format_of_status_and_missing_date)
        ]
        n_projects_after = len(long_format)
        logger.info(
            f"{n_projects_before - n_projects_after} {status} projects removed."
        )

        # set effective_date column to date_col for projects that == status
        long_format.loc[
            long_format["queue_status"].eq(status), "effective_date"
        ] = long_format[date_col]

    # Set end date to to null all projects.
    long_format["end_date"] = pd.NA

    # create a dataframe of withdrawn projects where the effective_date date is the date_entered_queue and the end date is the withdrawn_date
    withdrawn_active = long_format[long_format.queue_status.eq("withdrawn")].copy()
    withdrawn_active["queue_status"] = "new"
    withdrawn_active["effective_date"] = withdrawn_active["date_entered_queue"]
    withdrawn_active["end_date"] = withdrawn_active["withdrawn_date"]

    # create a dataframe of operational projects where the effective_date date is the date_entered_queue and the end date is the actual_completion_date
    operational_active = long_format[long_format.queue_status.eq("operational")].copy()
    operational_active["queue_status"] = "new"
    operational_active["effective_date"] = operational_active["date_entered_queue"]
    operational_active["end_date"] = operational_active["actual_completion_date"]

    # combine the withdrawn_active dataframe with the long_format dataframe
    long_format = pd.concat([long_format, withdrawn_active, operational_active])

    # drop withdrawn_date, actual_completion_date and date_entered_queue columns
    long_format = long_format.drop(
        columns=["withdrawn_date", "actual_completion_date", "date_entered_queue"]
    )

    # Map storage and renewable to "clean"
    long_format["resource_class"] = long_format["resource_class"].map(
        {"other": "other", "renewable": "clean", "fossil": "fossil", "storage": "clean"}
    )
    # Not doing the validation in dbcp.tests.validation because we
    # need access to iso_projects_long_format with withdrawn and operational
    # projects.
    validate_project_change_log(long_format, original_long_format)
    return long_format


def validate_project_change_log(
    iso_projects_change_log: pd.DataFrame, iso_projects_long_format: pd.DataFrame
):
    """Test the changelog and long format values roughly align."""
    # Grab the latest change log entry for each project
    iso_projects_change_log = iso_projects_change_log[
        iso_projects_change_log.end_date.isna()
    ]

    # The change log does not have all regions. Filter iso_projects_long_format to only include regions in the change log
    iso_projects_long_format = iso_projects_long_format[
        iso_projects_long_format["iso_region"].isin(CHANGE_LOG_REGIONS)
    ]

    # We expect some change in total projects count because not all projects have withdrawn and operational dates
    expected_n_projects_change = 0.05
    result_n_projects_change = abs(
        len(iso_projects_change_log) - len(iso_projects_long_format)
    ) / len(iso_projects_change_log)
    assert (
        result_n_projects_change < expected_n_projects_change
    ), f"Found unexpected change in total projects count: {result_n_projects_change}"

    # Create a dictionary of expected pct change for each iso_region
    expected_pct_change = pd.Series(
        {
            "CAISO": 0.02,
            "ISONE": 0.01,
            "MISO": 0.01,
            "NYISO": 0.18,  # A lot of withdrawn projects from the early 2000s are missing withdrawn and operational dates
            "PJM": 0.04,
            "SPP": 0.31,  # A lot of withdrawn projects from the early 2000s are missing withdrawn and operational dates
        }
    )

    # Calculate the pct change for each iso_region
    iso_projects_change_log_region_capacity = iso_projects_change_log.groupby(
        "iso_region"
    ).capacity_mw.sum()
    long_format_region_capacity = iso_projects_long_format.groupby(
        "iso_region"
    ).capacity_mw.sum()

    pct_change = (
        long_format_region_capacity - iso_projects_change_log_region_capacity
    ) / iso_projects_change_log_region_capacity
    assert pct_change.lt(
        expected_pct_change
    ).all(), f"Found unexpected pct change in iso_projects_long_format: {pct_change}"


def validate_iso_regions_change_log(
    iso_regions_change_log: pd.DataFrame, iso_projects_long_format: pd.DataFrame
):
    """Test the changelog and long format values roughly align."""
    # The change log does not have all regions. Filter iso_projects_long_format to only include regions in the change log
    iso_projects_long_format = iso_projects_long_format[
        iso_projects_long_format["iso_region"].isin(CHANGE_LOG_REGIONS)
    ]

    # Grab all new_*_n_project columns from the change log. All projects have "new" records in the change log
    new_cols = [
        col
        for col in iso_regions_change_log.columns
        if "n_projects" in col and "new" in col
    ]
    n_projects_iso_regions_change_log = iso_regions_change_log[new_cols].sum().sum()

    # We expect some change in total projects count because not all projects have withdrawn and operational dates
    # and resource_class
    expected_n_projects_change = 0.09
    result_n_projects_change = abs(
        n_projects_iso_regions_change_log - len(iso_projects_long_format)
    ) / len(iso_projects_long_format)
    assert (
        result_n_projects_change < expected_n_projects_change
    ), f"Found unexpected change in total projects count: {result_n_projects_change}"

    # Create a dictionary of expected pct change for each iso_region
    expected_pct_change = pd.Series(
        {
            "CAISO": 0.02,
            "ISONE": 0.01,
            "MISO": 0.01,
            "NYISO": 0.20,  # A lot of withdrawn projects from the early 2000s are missing withdrawn and operational dates
            "PJM": 0.04,
            "SPP": 0.31,  # A lot of withdrawn projects from the early 2000s are missing withdrawn and operational dates
        }
    )

    # Calculate the pct change for each iso_region
    new_cols = [
        col
        for col in iso_regions_change_log.columns
        if "capacity_mw" in col and "new" in col
    ]
    iso_projects_change_log_region_capacity = (
        iso_regions_change_log.groupby("iso_region")[new_cols].sum().sum(axis=1)
    )
    long_format_region_capacity = iso_projects_long_format.groupby(
        "iso_region"
    ).capacity_mw.sum()

    pct_change = (
        long_format_region_capacity - iso_projects_change_log_region_capacity
    ) / iso_projects_change_log_region_capacity
    assert pct_change.lt(
        expected_pct_change
    ).all(), f"Found unexpected pct change in iso_projects_long_format: {pct_change}"


def _pudl_eia860m_changelog(engine: sa.engine.Engine) -> pd.DataFrame:
    """Get the PUDL EIA860M changelog table."""
    with engine.connect() as con:
        pudl_eia860m_changelog = pd.read_sql_table(
            "pudl_eia860m_changelog", con, schema="data_warehouse"
        )
    return pudl_eia860m_changelog


def create_wide_geography_change_log(
    geography_change_log: pd.DataFrame,
    geography: str,
    status: str,
    resource_class: str,
    metric: str,
    date_range: tuple[str, str],
) -> pd.DataFrame:
    """
    Create a wide table of ISO Queue changes for a given status, resource_class and metric.

    Args:
        geography_change_log: project change log where each row is a snap shot of a geography
        geography: geography column to pivot on: county_id_fips or iso_region
        status: new, operational or withdrawn
        resource_class: clean, fossil or other
        metric: n_projects or capacity_mw
        date_range: tuple of start and end date to filter on
    Return:
        wide: wide table of ISO Queue changes
    """
    value_column = f"{status}_{resource_class}_{metric}"

    # Filter to date range
    geography_change_log = geography_change_log[
        geography_change_log.date.gt(date_range[0])
        & geography_change_log.date.lt(date_range[1])
    ]

    # Create the pivot table
    wide = geography_change_log.pivot(
        index=geography, columns="date", values=value_column
    )
    wide.columns = [col.strftime("%Y-%m") for col in wide.columns]
    wide = wide.fillna(0)
    return wide.reset_index()


def create_data_mart(
    engine: Optional[sa.engine.Engine] = None,
) -> dict[str, pd.DataFrame]:
    """Create projects datamart dataframe."""
    if engine is None:
        engine = get_sql_engine()

    all_projects_long_format = create_long_format(engine, active_projects_only=False)
    iso_projects_change_log = create_project_change_log(all_projects_long_format)

    # create counties and region change log tables
    data_marts = {}
    geographies = {"counties": "county_id_fips", "regions": "iso_region"}
    for geography, geography_columns in geographies.items():
        geography_change_log = create_geography_change_log(
            iso_projects_change_log, geography=geography_columns, freq="Q"
        )
        data_marts[f"iso_{geography}_change_log"] = geography_change_log

        metrics = ("n_projects", "capacity_mw")
        date_range = ("2022-01-01", datetime.now())
        status = "new"
        resource_class = "clean"
        for metric in metrics:
            data_marts[
                f"iso_{geography}_{status}_{resource_class}_{metric}_changelog"
            ] = create_wide_geography_change_log(
                geography_change_log,
                geography=geography_columns,
                status=status,
                resource_class=resource_class,
                metric=metric,
                date_range=date_range,
            )

    validate_iso_regions_change_log(
        data_marts["iso_regions_change_log"], all_projects_long_format
    )

    active_long_format = create_long_format(engine, active_projects_only=True)
    active_wide_format = _convert_long_to_wide(active_long_format)

    pudl_eia860m_changelog = _pudl_eia860m_changelog(engine)

    data_marts.update(
        {
            "iso_projects_long_format": active_long_format,
            "iso_projects_wide_format": active_wide_format,
            "iso_projects_change_log": iso_projects_change_log,
            "pudl_eia860m_changelog": pudl_eia860m_changelog,
        }
    )
    return data_marts


if __name__ == "__main__":
    # debugging entry point
    mart = create_data_mart()
    print("yeehaw")
