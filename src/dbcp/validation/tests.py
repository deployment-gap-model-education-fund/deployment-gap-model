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
    location_coverage = gridstatus_locations.county_id_fips.isna().sum() / len(
        gridstatus_locations
    )
    assert (
        location_coverage < 0.04
    ), "More than 4 percent of Grid Status locations could not be geocoded."


def test_iso_projects_sources(engine: Engine):
    """Check that the right resources come from the right sources."""
    # all offshore wind projects from the proprietary source
    proprietary_offshore = """
    SELECT
        source,
        count(*) as n_offshore
    from data_mart.iso_projects_long_format
    where resource_clean ~* 'offshore'
    group by 1
    """
    expected_source = {"proprietary"}
    offshore_test = pd.read_sql(
        proprietary_offshore, engine, index_col="source"
    ).squeeze(
        axis=1
    )  # make series
    actual_source = set(offshore_test.index)
    assert (
        actual_source == expected_source
    ), f"Found offshore wind projects from the wrong source. {offshore_test}"

    # all ISO projects from the gridstatus source
    iso_projects = """
    SELECT
        source,
        count(*) as n_iso
    from data_mart.iso_projects_long_format
    where iso_region ~* 'ercot|miso|nyiso|pjm|spp|isone'
    group by 1
    """
    expected_source = {"gridstatus"}  # region is currently NULL for offshore wind
    iso_test = pd.read_sql(iso_projects, engine, index_col="source").squeeze(axis=1)
    actual_source = set(iso_test.index)
    assert (
        actual_source == expected_source
    ), f"Found ISO projects from the wrong source. {iso_test}"
    # remaining projects from LBNL (non-ISO, non-offshore)
    return


def test_iso_projects_capacity_aggs(engine: Engine):
    """Check that the capacity aggregates equal the source tables."""
    data_mart_capacity = """
    select
        source,
        resource_clean,
        count(*) as n_project_locations,
        -- double count capacity when there are multiple locations. Simplifies the test
        sum(capacity_mw) as capacity_double_count_county
    from data_mart.iso_projects_long_format
    group by 1, 2
    order by 1, 2
    """
    # simplified reimplementation of the data_mart.iso_projects_long_format table.
    # This skips over the multi-county allocation stuff for simplicity.
    source_capacity = """
    with
    lbnl as (
        select
            'lbnl' as source,
            res.resource_clean,
            count(*) as n_project_locations,
            sum(res.capacity_mw) as capacity_double_count_county
        FROM data_warehouse.iso_projects as proj
        LEFT JOIN data_warehouse.iso_resource_capacity as res
        ON proj.project_id = res.project_id
        LEFT JOIN data_warehouse.iso_locations as loc
        ON proj.project_id = loc.project_id
        WHERE proj.region !~* 'ercot|miso|nyiso|pjm|spp|isone'
            AND resource_clean != 'Offshore Wind'
            AND proj.queue_status = 'active'
        group by 1, 2
    ),
    gridstatus as (
        select
            'gridstatus' as source,
            res.resource_clean,
            count(*) as n_project_locations,
            sum(res.capacity_mw) as capacity_double_count_county
        FROM data_warehouse.gridstatus_projects as proj
        LEFT JOIN data_warehouse.gridstatus_resource_capacity as res
        ON proj.project_id = res.project_id
        LEFT JOIN data_warehouse.gridstatus_locations as loc
        ON proj.project_id = loc.project_id
        WHERE resource_clean not in ('Offshore Wind', 'Transmission')
            AND proj.queue_status = 'active'
            AND proj.region ~* 'ercot|miso|nyiso|pjm|spp|isone'
        group by 1, 2
    ),
    offshore as (
        select
            'proprietary' as source,
            'Offshore Wind' as resource_clean,
            count(*) as n_project_locations,
            sum(proj.capacity_mw) as capacity_double_count_county
        FROM data_warehouse.offshore_wind_projects as proj
        LEFT JOIN data_warehouse.offshore_wind_cable_landing_association as loc
        ON proj.project_id = loc.project_id
        WHERE coalesce(proj.construction_status, 'TBD') IN
            ('Not started', 'Construction underway', 'Site assessment underway', 'TBD')
        group by 1, 2
    )
    select * from lbnl
    UNION ALL
    select * from gridstatus
    UNION ALL
    select * from offshore
    order by 1, 2
    """
    data_mart = pd.read_sql(
        data_mart_capacity, engine, index_col=["source", "resource_clean"]
    )
    source = pd.read_sql(
        source_capacity, engine, index_col=["source", "resource_clean"]
    )
    absolute_diff = data_mart - source
    relative_diff = absolute_diff / source

    assert (
        relative_diff.lt(1e-5).all().all()
    ), f"Aggregate resource metrics have a large relative difference: {relative_diff}"
    return


def test_iso_projects_data_mart(engine: Engine):
    """Test that data mart aggregates are close to simple aggregates of the source tables.

    These aggregates don't exactly match (I should figure out why), but they're within 0.1%.
    Probably either null handling or join logic on multi-location projects.
    """
    test_iso_projects_sources(engine)
    test_iso_projects_capacity_aggs(engine)
    return


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
    n_expected_counties = 2472
    assert notnull.any(axis=1).sum() == n_expected_counties, (
        "counties_wide_format has unexpected county coverage."
        f" Expected {n_expected_counties}, found {notnull.any(axis=1).sum()}"
    )


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
    test_iso_projects_data_mart(engine)
    test_county_commission_election_info(engine)


def validate_all(engine: Engine):
    """Run all validation tests."""
    validate_warehouse(engine)
    validate_data_mart(engine)


if __name__ == "__main__":
    # debugging entry point
    engine = get_sql_engine()
    validate_all(engine)
