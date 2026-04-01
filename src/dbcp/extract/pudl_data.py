"""Logic for extracing PUDL data."""

import pandas as pd

import dbcp


def _extract_eia860m_yearly_generators() -> pd.DataFrame:
    """Extract yearly generator history from PUDL resources."""
    pudl_resource_path = dbcp.helpers.get_pudl_resource(
        pudl_resource="out_eia__yearly_generators.parquet"
    )
    return pd.read_parquet(
        pudl_resource_path, engine="pyarrow", dtype_backend="numpy_nullable"
    )


def _extract_eia860m_changelog_generators() -> pd.DataFrame:
    """Extract the core_eia860m__changelog_generators parquet file from the PUDL resources."""
    pudl_resource_path = dbcp.helpers.get_pudl_resource(
        pudl_resource="core_eia860m__changelog_generators.parquet"
    )
    changelog_generators = pd.read_parquet(
        pudl_resource_path, engine="pyarrow", dtype_backend="numpy_nullable"
    )
    return changelog_generators


def _extract_eia860m_status_codes_definitions() -> pd.DataFrame:
    """Extract the core_eia__codes_operational_status parquet file from the PUDL resources."""
    pudl_resource_path = dbcp.helpers.get_pudl_resource(
        pudl_resource="core_eia__codes_operational_status.parquet"
    )
    status_codes_definitions = pd.read_parquet(
        pudl_resource_path, engine="pyarrow", dtype_backend="numpy_nullable"
    )
    return status_codes_definitions


def extract() -> dict[str, pd.DataFrame]:
    """Pull tables from pudl sqlite database.

    Returns:
        A dictionary of pandas DataFrames where the keys are the PUDL table names.

    """
    raw_pudl_tables = {}
    # dictionary of PUDL table names to names used in DGM data warehouse
    tables = {
        "eia860m__yearly_generators": _extract_eia860m_yearly_generators,
        "eia860m__changelog__generators": _extract_eia860m_changelog_generators,
        "eia860m__status_codes_definitions": _extract_eia860m_status_codes_definitions,
    }
    for dgm_table_name, extract_func in tables.items():
        raw_pudl_tables[dgm_table_name] = extract_func()
    return raw_pudl_tables
