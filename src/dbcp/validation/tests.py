"""Data Validation tests."""
import logging
from functools import lru_cache
from io import StringIO

import pandas as pd
from sqlalchemy.engine import Engine

from dbcp.data_mart.counties import (
    _add_derived_columns,
    _get_county_properties,
    _get_offshore_wind_extra_cols,
)
from dbcp.helpers import get_sql_engine
from dbcp.metadata.data_mart import counties_wide_format

logger = logging.getLogger(__name__)


def test_j40_county_fips_coverage(engine: Engine):
    """Test the expected differences between Justice40 county_id_fips and the Census county FIPS table.

    Justice40 uses 2010 Census tracts, whereas the Census county FIPS table uses 2020 Census tracts.
    There are a handful of differences between the two:
    - Census areas in Alaska were redefined
    - Shannon County, SD was renamed to Oglala Lakota County, SD and assigned a new FIPS 46113 -> 46102
    - Bedford City, VA was merged into Bedford County, VA (FIPS 51019). Its old FIPS 51515 is defunct.
    """
    query = """
    WITH
    j40_counties as (
        SELECT
            DISTINCT SUBSTRING("tract_id_fips", 1, 5) as county_id_fips
        FROM data_warehouse.justice40_tracts
    )
    select
        j.county_id_fips as j40_fips,
        c.county_id_fips as c_fips
    from j40_counties as j
    full outer join data_warehouse.county_fips as c
    USING (county_id_fips)
    where j.county_id_fips is null
    or c.county_id_fips is null
    order by j40_fips desc nulls last, c_fips desc nulls last
    """
    expected = pd.read_csv(
        StringIO(
            """j40_fips,c_fips
51515,null
46113,null
02270,null
02261,null
null,46102
null,02158
null,02066
null,02063
"""
        ),
        dtype="string",
        header=0,
    )
    actual = pd.read_sql(query, engine).astype("string")
    pd.testing.assert_frame_equal(actual, expected)


def test_gridstatus_fips_coverage(engine: Engine):
    """Make sure we have high coverage for county_id_fips codes or gridstatus_projects."""
    with engine.connect() as con:
        gridstatus_locations = pd.read_sql_table(
            "gridstatus_locations", con, schema="data_warehouse"
        )
    assert (
        gridstatus_locations.county_id_fips.isna().sum() / len(gridstatus_locations)
        < 0.02
    ), "More than 2 percent of Grid Status locations could not be geocoded."


def test_iso_projects_data_mart_aggregates_are_close(engine: Engine):
    """Test that data mart aggregates are close to simple aggregates of the source tables.

    These aggregates don't exactly match (I should figure out why), but they're within 0.1%.
    Probably either null handling or join logic on multi-location projects.
    """
    data_mart_query = """
    SELECT
        count(*) as n_project_location_resource,
        sum(capacity_mw::float * frac_locations_in_county) as total_capacity
    from data_mart.iso_projects_long_format
    """
    source_query = """
    WITH
    iso_loc as (
        SELECT
            project_id,
            county_id_fips,
            1.0 / count(*) OVER (PARTITION BY project_id) as frac_county
        FROM data_warehouse.iso_locations
        FULL OUTER JOIN data_warehouse.iso_projects
        USING (project_id)
    ),
    iso as (
        select
            -- count project-county-resource items, not projects
            count(*) as n_project_location_resource,
            sum(capacity_mw * frac_county ) as total_capacity
        from data_warehouse.iso_resource_capacity
        full outer join iso_loc
        USING (project_id)
        where resource != 'Offshore Wind'
    ),
    offshore_proj as (
        select
            -- count project-county-resource items (only one resource here)
            -- not project-location-resource items or projects themselves
            count(distinct (project_id, loc.county_id_fips)) as n_project_location_resource,
            NULL::REAL as total_capacity
        from data_warehouse.offshore_wind_projects
        full outer join data_warehouse.offshore_wind_cable_landing_association
        USING (project_id)
        full outer join data_warehouse.offshore_wind_locations as loc
        USING (location_id)
        where construction_status != 'Operating'
    ),
    offshore_cap as (
        SELECT
            NULL::INTEGER as n_project_location_resource,
            -- calc total capacity separately because the capacity is split
            -- between locations. This is an easy way to get the same total
            -- without re-implementing the splitting logic.
            sum(capacity_mw) as total_capacity
        from data_warehouse.offshore_wind_projects
        where construction_status != 'Operating'
    ),
    combined as (
        select * from iso
        UNION ALL
        select * from offshore_proj
        UNION ALL
        select * from offshore_cap
    )
    select
        sum(n_project_location_resource) as n_project_location_resource,
        sum(total_capacity) as total_capacity
    from combined
    """
    source_totals = pd.read_sql(source_query, engine)
    data_mart_totals = pd.read_sql(data_mart_query, engine)
    absolute_diff = data_mart_totals - source_totals
    relative_diff = absolute_diff / source_totals
    assert (
        relative_diff.lt(0.0015).all().all()
    ), f"relative_difference too large: {relative_diff}"


def test_county_commission_election_info(engine: Engine):
    """Check total_n_seats is >= total_n_races."""
    with engine.connect() as con:
        df = pd.read_sql_table(
            "county_commission_election_info", con, schema="data_mart"
        ).convert_dtypes()
    assert (
        df.next_primary_total_n_seats >= df.next_primary_total_n_races
    ).all(), "Found more races than seats in county_commission_election_info!"
    assert (
        df.next_general_total_n_seats >= df.next_general_total_n_races
    ).all(), "Found more races than seats in county_commission_election_info!"
    assert (
        df.next_run_off_total_n_seats >= df.next_run_off_total_n_races
    ).all(), "Found more races than seats in county_commission_election_info!"


def test_county_wide_coverage(engine: Engine):
    """Check how many counties have technical data in counties_wide_format."""
    cols_to_fetch = _get_non_county_cols_from_wide_format(engine)
    query = f"SELECT {','.join(cols_to_fetch)} FROM data_mart.counties_wide_format"
    df = pd.read_sql(query, engine)
    n_counties = pd.read_sql(
        "SELECT count(*) FROM data_warehouse.county_fips", engine
    ).squeeze()
    assert (
        df.shape[0] == n_counties
    ), "counties_wide_format does not contain all counties"
    notnull = df.notnull()
    assert (
        notnull.any(axis=1).sum() == 2375
    ), f"counties_wide_format has unexpected county coverage: {notnull[notnull.any(axis=1)]}"


def test_county_long_vs_wide(engine: Engine):
    """Check that the long and wide formats have the same data, where appropriate."""
    offshore_wind_extra_cols = _get_offshore_wind_extra_cols(engine).columns
    # don't count the extra offshore cols because they are not present in long format (3 extra counties covered)
    cols_to_fetch = list(
        _get_non_county_cols_from_wide_format(engine).difference(
            offshore_wind_extra_cols
        )
    ) + ["county_id_fips"]
    query = f"SELECT {','.join(cols_to_fetch)} FROM data_mart.counties_wide_format"
    wide_format_technical = pd.read_sql(query, engine).set_index("county_id_fips")

    # filter long_format for only the same resources as wide_format
    resources_to_keep = {  # wide_format excludes some categories by client request
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
        "Synthetic Fertilizers",
        "Petrochemicals and Plastics",
    }
    string_wrapped = (f"'{item}'" for item in resources_to_keep)
    shorter_long_query = f"SELECT * FROM data_mart.counties_long_format WHERE resource_or_sector in ({','.join(string_wrapped)})"
    shorter_long_format = pd.read_sql(shorter_long_query, engine)

    # county coverage ~~of technical data~~ should be the same (not true for
    # county-level data because wide_format includes counties that don't have
    # any projects)
    n_counties_wide = wide_format_technical.index[
        wide_format_technical.notnull().any(axis=1)
    ].nunique()
    n_counties_long = shorter_long_format["county_id_fips"].nunique()
    assert (
        n_counties_wide == n_counties_long
    ), "counties_wide_format and counties_long_format have different county coverage"

    # check project counts
    def _condition(col: str) -> bool:
        is_count = col.endswith("_count")
        # want to remove category aggregates to avoid double counting with the individual categories
        is_combined_aggregate = (
            col.startswith("fossil_")
            or col.startswith("renewable_")
            or col.startswith("infra_total_")
        )
        return is_count and not is_combined_aggregate

    count_cols = [col for col in wide_format_technical.columns if _condition(col)]
    wide_project_counts = wide_format_technical.loc[:, count_cols].sum().sum()
    long_project_counts = shorter_long_format["facility_count"].sum()
    assert (
        long_project_counts == wide_project_counts
    ), "counties_long_format has fewer projects than counties_wide_format"


def test_manual_ordinance_fips_coverage(engine: Engine):
    """Check that manual_ordinances and county_fips have identical FIPS."""
    query = """
    SELECT
        m.county_id_fips as manual_fips,
        c.county_id_fips as county_fips
    FROM data_mart.manual_ordinances as m
    FULL OUTER JOIN data_warehouse.county_fips as c
    USING (county_id_fips)
    WHERE m.county_id_fips is null OR c.county_id_fips is null
    """
    actual = pd.read_sql(query, engine)
    assert actual.empty, "Found mismatched FIPS in manual_ordinances"


@lru_cache(maxsize=1)
def _get_non_county_cols_from_wide_format(engine: Engine) -> pd.Index:
    """Get the columns from counties_wide_format that are not derived from county-level data."""
    wide_cols = pd.Index([col.name for col in counties_wide_format.columns])
    county_level_cols = _add_derived_columns(_get_county_properties(engine)).columns
    cols_to_fetch = wide_cols.difference(county_level_cols)
    return cols_to_fetch


def validate_warehouse(engine: Engine):
    """Run data warehouse validation tests."""
    logger.info("Validating data warehouse")
    test_j40_county_fips_coverage(engine)
    test_gridstatus_fips_coverage(engine)


def validate_data_mart(engine: Engine):
    """Run data mart validation tests."""
    logger.info("Validating data mart")
    test_county_long_vs_wide(engine)
    test_county_wide_coverage(engine)
    # TODO (bendnorman): This is currently failing but a small percentage
    # test_iso_projects_data_mart_aggregates_are_close(engine)
    test_county_commission_election_info(engine)


def validate_all(engine: Engine):
    """Run all validation tests."""
    validate_warehouse(engine)
    validate_data_mart(engine)


if __name__ == "__main__":
    engine = get_sql_engine()
    validate_all(engine)
