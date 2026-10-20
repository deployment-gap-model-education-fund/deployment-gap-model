"""Test DBCP helper functions."""

import hashlib
from pathlib import Path

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


class FakeFilesystem:
    """Minimal stand-in for an fsspec filesystem that only implements ``info``."""

    def __init__(self, info: dict):
        """Store the object metadata returned by ``info``."""
        self._info = info

    def info(self, path: str) -> dict:
        """Return canned object metadata, ignoring ``path``."""
        return self._info


@pytest.fixture
def local_file(tmp_path: Path) -> Path:
    """Write a small local file used to compare against fake remote metadata."""
    path = tmp_path / "resource.parquet"
    path.write_bytes(b"pudl resource contents")
    return path


def test_local_file_matches_remote_when_checksums_match(local_file: Path):
    """Matching size and ETag means the cached file is up to date."""
    etag = hashlib.md5(local_file.read_bytes()).hexdigest()  # noqa: S324
    fs = FakeFilesystem(
        {"size": local_file.stat().st_size, "ETag": f'"{etag}"'},
    )

    assert (
        dbcp.helpers._local_file_matches_remote(local_file, "s3://bucket/key", fs)
        is True
    )


def test_local_file_matches_remote_false_on_size_mismatch(local_file: Path):
    """A different size short circuits before the checksum is computed."""
    fs = FakeFilesystem({"size": local_file.stat().st_size + 1})

    assert (
        dbcp.helpers._local_file_matches_remote(local_file, "s3://bucket/key", fs)
        is False
    )


def test_local_file_matches_remote_false_on_checksum_mismatch(local_file: Path):
    """Same size but a different ETag means the cached file is stale."""
    stale_etag = hashlib.md5(b"different contents!!!!").hexdigest()  # noqa: S324
    fs = FakeFilesystem({"size": local_file.stat().st_size, "ETag": f'"{stale_etag}"'})

    assert (
        dbcp.helpers._local_file_matches_remote(local_file, "s3://bucket/key", fs)
        is False
    )


@pytest.mark.parametrize(
    "etag",
    [
        "",
        '"d41d8cd98f00b204e9800998ecf8427e-3"',  # multipart upload ETag
    ],
)
def test_local_file_matches_remote_raises_without_usable_checksum(
    local_file: Path, etag: str
):
    """Missing or multipart ETags can't be compared to a plain MD5."""
    fs = FakeFilesystem({"size": local_file.stat().st_size, "ETag": etag})

    with pytest.raises(RuntimeError, match="Can't get checksum"):
        dbcp.helpers._local_file_matches_remote(local_file, "s3://bucket/key", fs)
