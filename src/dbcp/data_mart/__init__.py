"""Modules to create tables in the 'data mart' for direct use by users."""

import importlib
import logging
import pkgutil

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa

import dbcp
from dbcp.constants import OUTPUT_DIR
from dbcp.helpers import enforce_dtypes, psql_insert_copy
from dbcp.validation.tests import validate_data_mart

logger = logging.getLogger(__name__)


def write_to_postgres_and_parquet(
    data_marts: dict[str, pd.DataFrame], engine: sa.engine.Engine, schema_name: str
):
    """Write data mart tables from a schema to postgres and parquet."""
    # Setup postgres
    with engine.connect() as con:
        engine.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    # Delete any existing tables, and create them anew
    metadata = dbcp.helpers.get_schema_sql_alchemy_metadata(schema_name)
    metadata.drop_all(engine)
    metadata.create_all(engine)

    parquet_dir = OUTPUT_DIR / f"{schema_name}"

    # Load table into postgres and parquet
    with engine.connect() as con:
        for table in metadata.sorted_tables:
            logger.info(f"Load {table.name} to postgres.")
            df = dbcp.helpers.trim_columns_length(data_marts[table.name])
            df = enforce_dtypes(df, table.name, schema_name)
            df.to_sql(
                name=table.name,
                con=con,
                if_exists="append",
                index=False,
                schema=schema_name,
                method=psql_insert_copy,
                chunksize=5000,  # adjust based on memory capacity
            )
            schema = dbcp.helpers.get_pyarrow_schema_from_metadata(
                table.name, schema_name
            )
            pa_table = pa.Table.from_pandas(df, schema=schema)
            pq.write_table(pa_table, parquet_dir / f"{table.name}.parquet")


def create_data_marts():  # noqa: max-complexity=11
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
    for schema_name in ["data_mart", "private_data_mart"]:
        write_to_postgres_and_parquet(
            data_marts=data_marts, engine=engine, schema_name=schema_name
        )

    validate_data_mart(engine=engine)
