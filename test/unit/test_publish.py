"""Test for publish command."""

from unittest.mock import MagicMock, patch

import pandas as pd
from upath import UPath

from dbcp.commands.publish import load_tables_to_postgres


def test_load_tables_to_postgres_generates_deployment_metadata():
    """Test that deployment metadata is generated correctly for new, changed, and unchanged tables."""
    current_metadata = pd.DataFrame(
        [
            {
                "table_name": "table_unchanged__v1",
                "last_modified_deployment_id": "old-version",
                "last_modified": pd.Timestamp("2026-01-01 00:00:00"),
            },
            {
                "table_name": "table_changed__v1",
                "last_modified_deployment_id": "old-version",
                "last_modified": pd.Timestamp("2026-01-02 00:00:00"),
            },
        ]
    )

    table_unchanged = MagicMock()
    table_unchanged.name = "table_unchanged__v1"
    table_changed = MagicMock()
    table_changed.name = "table_changed__v1"
    table_new = MagicMock()
    table_new.name = "table_new__v1"

    schema = MagicMock()
    schema.value = "data_mart"
    metadata = MagicMock()
    metadata.sorted_tables = [table_unchanged, table_changed, table_new]

    def fake_check_versions(old_version, new_version):
        if "table_unchanged__v1.parquet" in str(new_version):
            return True
        if "table_changed__v1.parquet" in str(new_version):
            return False
        if "table_new__v1.parquet" in str(new_version):
            return False
        raise AssertionError(f"Unexpected path: {new_version}")

    mock_deployment_metadata_model = MagicMock()
    mock_validated_df = MagicMock()
    mock_deployment_metadata_model.validate.return_value = mock_validated_df

    mock_output_metadata = MagicMock()
    mock_output_metadata.output_directory = UPath("gs://bucket/old-version")

    with (
        patch("dbcp.commands.publish.get_postgres_engine") as mock_get_engine,
        patch("dbcp.commands.publish.pd.read_sql", return_value=current_metadata),
        patch("dbcp.commands.publish.pd.read_parquet") as mock_read_parquet,
        patch(
            "dbcp.commands.publish.check_table_versions_equivalent",
            side_effect=fake_check_versions,
        ),
        patch("dbcp.commands.publish.write_to_sql") as mock_write_to_sql,
        patch(
            "dbcp.commands.publish.get_schema_sql_alchemy_metadata",
            return_value=metadata,
        ),
        patch("dbcp.commands.publish.SchemaName", [schema]),
        patch(
            "dbcp.commands.publish.DeploymentMetadata", mock_deployment_metadata_model
        ),
        patch(
            "dbcp.commands.publish.OutputMetadata.from_version",
            return_value=mock_output_metadata,
        ) as mock_from_version,
    ):
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        load_tables_to_postgres(
            output_directory=UPath("gs://bucket/new-version"),
            target="dev",
            version="new-version",
        )

    mock_from_version.assert_any_call("old-version")

    assert mock_write_to_sql.call_count == 2
    assert mock_read_parquet.call_count == 2

    written_tables = [
        call.kwargs["table_name"] for call in mock_write_to_sql.call_args_list
    ]
    assert written_tables == ["table_changed__v1", "table_new__v1"]

    mock_deployment_metadata_model.validate.assert_called_once()
    validated_df = mock_deployment_metadata_model.validate.call_args.args[0]
    assert list(validated_df["table_name"]) == [
        "table_unchanged__v1",
        "table_changed__v1",
        "table_new__v1",
    ]
    assert list(validated_df["last_modified_deployment_id"]) == [
        "old-version",
        "new-version",
        "new-version",
    ]
