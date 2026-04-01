"""Logic for extracing PUDL data."""

import pandas as pd

import dbcp
from dbcp.constants import PUDL_LATEST_YEAR


def _extract_eia860m_annual_generators() -> pd.DataFrame:
    """Extract eia860m__annual__generators table from PUDL resources.

    Returns:
        The eia860m__annual__generators table.

    """
    pudl_resource_path = dbcp.helpers.get_pudl_resource(
        pudl_resource="out_eia__yearly_generators.parquet"
    )
    generators = pd.read_parquet(
        pudl_resource_path, engine="pyarrow", dtype_backend="numpy_nullable"
    )

    # convert columns with 'date' in the name to datetime
    # TODO: Use dtype_backend="pyarrow" when we update to pandas >= 2.0
    date_columns = [col for col in generators.columns if "date" in col]
    for col in date_columns:
        generators[col] = pd.to_datetime(generators[col])

    # filter generators where report_year >= PUDL_LATEST_YEAR and < PUDL_LATEST_YEAR+1
    generators = generators[
        (generators.report_date.dt.year >= PUDL_LATEST_YEAR)
        & (generators.report_date.dt.year < PUDL_LATEST_YEAR + 1)
    ]
    return generators


def _extract_eia860m_changelog_generators() -> pd.DataFrame:
    """Extract the core_eia860m__changelog_generators parquet file from the PUDL resources."""
    pudl_resource_path = dbcp.helpers.get_pudl_resource(
        pudl_resource="core_eia860m__changelog_generators.parquet"
    )
    changelog_generators = pd.read_parquet(
        pudl_resource_path, engine="pyarrow", dtype_backend="numpy_nullable"
    )
    return changelog_generators


def _extract_eia860m_operational_status_codes() -> pd.DataFrame:
    """Extract the core_eia__codes_operational_status parquet file from the PUDL resources."""
    pudl_resource_path = dbcp.helpers.get_pudl_resource(
        pudl_resource="core_eia__codes_operational_status.parquet"
    )
    operational_status_codes = pd.read_parquet(
        pudl_resource_path, engine="pyarrow", dtype_backend="numpy_nullable"
    )
    return operational_status_codes


def extract() -> dict[str, pd.DataFrame]:
    """Pull tables from pudl sqlite database.

    Returns:
        A dictionary of pandas DataFrames where the keys are the PUDL table names.

    """
    raw_pudl_tables = {}
    # dictionary of PUDL table names to names used in DGM data warehouse
    tables = {
        "eia860m__annual__generators": _extract_eia860m_annual_generators,
        "eia860m__changelog__generators": _extract_eia860m_changelog_generators,
        "eia860m__operational_status_codes": _extract_eia860m_operational_status_codes,
    }
    for dgm_table_name, extract_func in tables.items():
        raw_pudl_tables[dgm_table_name] = extract_func()
    return raw_pudl_tables
