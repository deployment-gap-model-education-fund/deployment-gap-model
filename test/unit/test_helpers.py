"""Test DBCP helper functions."""

import pandas as pd
import pytest
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


@pytest.mark.parametrize(
    "old_df,new_df,expected",
    [
        (
            pd.DataFrame({"a": [1, 2], "b": [1.0, 2.0]}),
            pd.DataFrame({"a": [1, 2], "b": [1.0, 2.0]}),
            True,
        ),
        (
            pd.DataFrame({"a": [1, 2], "b": [3, 4]}),
            pd.DataFrame({"b": [3, 4], "a": [1, 2]}),
            True,
        ),
        (
            pd.DataFrame({"a": [1.0, 2.0]}),
            pd.DataFrame({"a": [1.0 + 1e-6, 2.0 - 1e-6]}),
            True,
        ),
        (pd.DataFrame({"a": [1, 2]}), pd.DataFrame({"a": [1, 3]}), False),
        (pd.DataFrame({"a": [1, 2]}), pd.DataFrame({"a": [1]}), False),
        (pd.DataFrame({"a": [1, 2]}), pd.DataFrame({"a": [1.0, 2.0]}), False),
    ],
)
def test_check_table_versions_equivalent(tmpdir, old_df, new_df, expected):
    """Test that `check_table_versions_equivalent` works as expected with test inputs."""
    old_path = tmpdir.join("old.parquet")
    new_path = tmpdir.join("new.parquet")
    old_df.to_parquet(str(old_path))
    new_df.to_parquet(str(new_path))

    assert (
        dbcp.helpers.check_table_versions_equivalent(str(old_path), str(new_path))
        is expected
    )
