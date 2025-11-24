"""SQL Alchemy metadata for the private data mart tables."""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
)

metadata = MetaData()
schema = "private_data_mart"

fyi_projects_long_format = Table(
    "fyi_projects_long_format",
    metadata,
    # PK is project_id, county_id_fips, resource_clean
    Column("state", String),
    Column("county", String),
    Column("county_id_fips", String),
    Column("queue_id", String),
    Column("resource_clean", String, nullable=False),
    Column("project_id", String, nullable=False),
    Column("date_proposed_online", DateTime),
    Column("power_market", String),
    Column("iso", String),
    Column("developer", String),
    Column("entity", String),
    Column("interconnection_status", String),
    Column("point_of_interconnection", String),
    Column("project_name", String),
    Column("date_entered_queue", DateTime),
    Column("queue_status", String, nullable=False),
    Column("utility", String),
    Column("capacity_mw", Float),
    Column("state_id_fips", String),
    Column("state_permitting_type", String),
    Column("co2e_tonnes_per_year", Float),
    Column("ordinance_earliest_year_mentioned", Float),
    Column("ordinance_jurisdiction_name", String),
    Column("ordinance_jurisdiction_type", String),
    Column("ordinance_text", String),
    Column("ordinance_via_reldi", Boolean, nullable=False),
    Column("ordinance_via_solar_nrel", Boolean),
    Column("ordinance_via_wind_nrel", Boolean),
    Column("ordinance_via_nrel_is_de_facto", Boolean),
    Column("ordinance_via_self_maintained", Boolean),
    Column("ordinance_is_restrictive", Boolean),
    Column("is_hybrid", Boolean, nullable=False),
    Column("is_actionable", Boolean),
    Column("is_nearly_certain", Boolean),
    Column("resource_class", String),
    Column("frac_locations_in_county", Float, nullable=False),
    Column("source", String, nullable=False),
    schema=schema,
)

fyi_counties_proposed_clean_projects = Table(
    "fyi_counties_proposed_clean_projects",
    metadata,
    Column("county_id_fips", String),
    Column("resource_clean", String),
    Column("renewable_and_battery_proposed_capacity_mw", Float),
    Column("facility_count", Integer),
    schema=schema,
)

fyi_counties_proposed_clean_projects_wide = Table(
    "fyi_counties_proposed_clean_projects_wide",
    metadata,
    Column("county_id_fips", String, primary_key=True),
    Column("battery_storage_mw", String),
    Column("onshore_wind_mw", String),
    Column("solar_mw", Float),
    Column("total_proposed_capacity_mw", Float),
    schema=schema,
)
