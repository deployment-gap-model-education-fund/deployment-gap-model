"""Test DBCP helper functions."""

import sqlalchemy as sa

import dbcp
from dbcp.metadata import SchemaName


class TestBQSchemaHelpers:
    """Test the helper functions that convert sqlalchemy schemas to bigquery schemas."""

    def test_get_schema_sql_alchemy_metadata(self):
        """Ensure we get the expected metadata object."""
        metadata = dbcp.helpers.get_schema_sql_alchemy_metadata(
            SchemaName.DATA_WAREHOUSE
        )
        assert isinstance(metadata, sa.MetaData)
        assert "data_warehouse._eia860m__changelog__generators" in metadata.tables

    def test_get_bq_schema_from_metadata(self):
        """Ensure we get the expected Bigquery schema."""
        expected_bq_schema = [
            {"name": "county_id_fips", "mode": "REQUIRED", "type": "STRING"},
            {"name": "state_id_fips", "mode": "REQUIRED", "type": "STRING"},
            {"name": "county_name", "mode": "REQUIRED", "type": "STRING"},
            {"name": "county_name_long", "mode": "REQUIRED", "type": "STRING"},
            {"name": "functional_status", "mode": "REQUIRED", "type": "STRING"},
            {"name": "land_area_km2", "mode": "REQUIRED", "type": "FLOAT"},
            {"name": "water_area_km2", "mode": "REQUIRED", "type": "FLOAT"},
            {"name": "centroid_latitude", "mode": "REQUIRED", "type": "FLOAT"},
            {"name": "centroid_longitude", "mode": "REQUIRED", "type": "FLOAT"},
            {"name": "raw_tribal_land_frac", "mode": "REQUIRED", "type": "FLOAT"},
            {"name": "tribal_land_frac", "mode": "REQUIRED", "type": "FLOAT"},
        ]

        bq_schema = dbcp.helpers.get_bq_schema_from_metadata(
            "census__county_fips", SchemaName.DATA_WAREHOUSE
        )
        assert bq_schema == expected_bq_schema
