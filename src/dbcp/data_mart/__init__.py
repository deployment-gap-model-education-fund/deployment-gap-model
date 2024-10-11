"""Modules to create tables in the 'data mart' for direct use by users."""

import importlib
import logging
import pkgutil

import pandas as pd

import dbcp
from dbcp.constants import OUTPUT_DIR
from dbcp.helpers import enforce_dtypes, psql_insert_copy
from dbcp.metadata.data_mart import metadata
from dbcp.validation.tests import validate_data_mart

logger = logging.getLogger(__name__)


def create_data_marts(args):  # noqa: max-complexity=11
    """Collect and load all data mart tables to data warehouse."""
    engine = dbcp.helpers.get_sql_engine()
    data_marts = {}
    modules_to_skip = {
        "helpers",  # helper code; no tables
        "co2_dashboard",  # obsolete but code imported elsewhere
    }

    for module_info in pkgutil.iter_modules(__path__):
        if module_info.name in modules_to_skip:
            continue
        module = importlib.import_module(f"{__name__}.{module_info.name}")
        try:
            data = module.create_data_mart(engine=engine)
        except AttributeError:
            raise AttributeError(
                f"{module_info.name} has no attribute 'create_data_mart'."
                "Make sure the data mart module implements create_data_mart function."
            )
        if isinstance(data, pd.DataFrame):
            assert (
                module_info.name not in data_marts.keys()
            ), f"Key {module_info.name} already exists in data mart"
            data_marts[module_info.name] = data
        elif isinstance(data, dict):
            assert (
                len([key for key in data.keys() if key in data_marts.keys()]) == 0
            ), f"Dict key from {module_info.name} already exists"
            data_marts.update(data)
        else:
            raise TypeError(
                f"Expecting pd.DataFrame or dict of dataframes. Got {type(data)}"
            )

    # Setup postgres
    with engine.connect() as con:
        engine.execute("CREATE SCHEMA IF NOT EXISTS data_mart")

    # Create the schemas
    metadata.drop_all(engine)
    metadata.create_all(engine)

    parquet_dir = OUTPUT_DIR / "data_mart"

    # Load table into postgres and parquet
    with engine.connect() as con:
        for table in metadata.sorted_tables:
            logger.info(f"Load {table.name} to postgres.")
            df = enforce_dtypes(data_marts[table.name], table.name, "data_mart")
            df = dbcp.helpers.trim_columns_length(df)
            df.to_sql(
                name=table.name,
                con=con,
                if_exists="append",
                index=False,
                schema="data_mart",
                method=psql_insert_copy,
            )

            df.to_parquet(parquet_dir / f"{table.name}.parquet", index=False)

    validate_data_mart(engine=engine)
