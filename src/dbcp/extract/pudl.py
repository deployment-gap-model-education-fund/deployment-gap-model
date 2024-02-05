"""Logic for extracing PUDL data."""
import pandas as pd
import sqlalchemy as sa

import dbcp


def _extract_pudl_generators(pudl_engine: sa.engine.base.Engine) -> pd.DataFrame:
    """Extract pudl_generators table from pudl sqlite database.

    Args:
        pudl_engine: The pudl sqlite database engine.

    Returns:
        The pudl_generators table.
    """
    with pudl_engine.connect() as con:
        pudl_generators = pd.read_sql(
            "SELECT * FROM out_eia__yearly_generators WHERE report_date >= '2022-01-01' AND report_date < '2023-01-01'",
            con,
        )
    return pudl_generators


def extract() -> dict[str, pd.DataFrame]:
    """Pull tables from pudl sqlite database.

    Returns:
        A dictionary of pandas DataFrames where the keys are the PUDL table names.
    """
    pudl_sqlite_path = dbcp.helpers.get_pudl_resource("pudl.sqlite.gz")

    raw_pudl_tables = {}

    pudl_engine = sa.create_engine(f"sqlite:////{pudl_sqlite_path}")
    # dictionary of PUDL table names to names used in DGM data warehouse
    tables = {"pudl_generators": _extract_pudl_generators}
    for dgm_table_name, extract_func in tables.items():
        raw_pudl_tables[dgm_table_name] = extract_func(pudl_engine)
    return raw_pudl_tables
