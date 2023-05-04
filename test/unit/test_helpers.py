"""Test DBCP helper functions."""
import pytest
import sqlalchemy as sa

import dbcp


def test_missing_get_db_schema_tables():
    """Test when schema does not exist or doesn't have any tables."""
    engine = dbcp.helpers.get_sql_engine()
    with pytest.raises(ValueError):
        _ = dbcp.helpers.get_db_schema_tables(engine, "i_dont_exist")


class TestBQSchemaHelpers:
    """Test the helper functions that convert sqlalchemy schemas to bigquery schemas."""

    def test_get_schema_sql_alchemy_metadata(self):
        """Ensure we get the expected metadata object."""
        metadata = dbcp.helpers.get_schema_sql_alchemy_metadata("data_warehouse")
        assert isinstance(metadata, sa.MetaData)
        assert "data_warehouse.mcoe" in metadata.tables.keys()

    def test_get_schema_sql_alchemy_metadata_exception(self):
        """Make sure an exception is raised when an invalid schema is requested."""
        with pytest.raises(ValueError):
            _ = dbcp.helpers.get_schema_sql_alchemy_metadata("data_lake")

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
            {"name": "tribal_land_percent", "mode": "REQUIRED", "type": "FLOAT"},
        ]

        bq_schema = dbcp.helpers.get_bq_schema_from_metadata(
            "county_fips", "data_warehouse"
        )
        assert bq_schema == expected_bq_schema
