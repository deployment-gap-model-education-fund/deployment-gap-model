"""Logic for extracing PUDL data."""

import pandas as pd

import dbcp
from dbcp.constants import PUDL_LATEST_YEAR


def _extract_pudl_generators() -> pd.DataFrame:
    """Extract pudl_generators table from pudl sqlite database.

    Returns:
        The pudl_generators table.
    """
    pudl_resource_path = dbcp.helpers.get_pudl_resource(
        pudl_resource="out_eia__yearly_generators.parquet"
    )
    pudl_generators = pd.read_parquet(
        pudl_resource_path, engine="pyarrow", dtype_backend="numpy_nullable"
    )

    # convert columns with 'date' in the name to datetime
    # TODO: Use dtype_backend="pyarrow" when we update to pandas >= 2.0
    date_columns = [col for col in pudl_generators.columns if "date" in col]
    for col in date_columns:
        pudl_generators[col] = pd.to_datetime(pudl_generators[col])

    # filter generators where report_year >= PUDL_LATEST_YEAR and < PUDL_LATEST_YEAR+1
    pudl_generators = pudl_generators[
        (pudl_generators.report_date.dt.year >= PUDL_LATEST_YEAR)
        & (pudl_generators.report_date.dt.year < PUDL_LATEST_YEAR + 1)
    ]
    return pudl_generators


def _extract_pudl_eia860m_changelog() -> pd.DataFrame:
    """Extract the core_eia860m__changelog_generators parquet file from the PUDL resources."""
    pudl_resource_path = dbcp.helpers.get_pudl_resource(
        pudl_resource="core_eia860m__changelog_generators.parquet"
    )
    pudl_eia860m_changelog = pd.read_parquet(
        pudl_resource_path, engine="pyarrow", dtype_backend="numpy_nullable"
    )
    return pudl_eia860m_changelog


def _extract_pudl_eia860m_status_codes() -> pd.DataFrame:
    """Extract the core_eia__codes_operational_status parquet file from the PUDL resources."""
    pudl_resource_path = dbcp.helpers.get_pudl_resource(
        pudl_resource="core_eia__codes_operational_status.parquet"
    )
    pudl_eia860m_status_codes = pd.read_parquet(
        pudl_resource_path, engine="pyarrow", dtype_backend="numpy_nullable"
    )
    return pudl_eia860m_status_codes


def extract() -> dict[str, pd.DataFrame]:
    """Pull tables from pudl sqlite database.

    Returns:
        A dictionary of pandas DataFrames where the keys are the PUDL table names.
    """
    raw_pudl_tables = {}
    # dictionary of PUDL table names to names used in DGM data warehouse
    tables = {
        "pudl_generators": _extract_pudl_generators,
        "pudl_eia860m_changelog": _extract_pudl_eia860m_changelog,
        "pudl_eia860m_status_codes": _extract_pudl_eia860m_status_codes,
    }
    for dgm_table_name, extract_func in tables.items():
        raw_pudl_tables[dgm_table_name] = extract_func()
    return raw_pudl_tables
