"""Modules to create tables in the 'data mart' for direct use by users."""

import importlib
import logging
import pkgutil

import pandas as pd

import dbcp

logger = logging.getLogger(__name__)


def create_data_marts(args):
    """Collect and load all data mart tables to data warehouse."""
    data_marts = {}
    for module_info in pkgutil.iter_modules(__path__):
        module = importlib.import_module(f"{__name__}.{module_info.name}")
        try:
            data = module.create_data_mart()
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
    engine = dbcp.helpers.get_sql_engine()
    with engine.connect() as con:
        engine.execute("CREATE SCHEMA IF NOT EXISTS data_mart")

    # Load table into postgres
    with engine.connect() as con:
        for table_name, df in data_marts.items():
            logger.info(f"Load {table_name} to postgres.")
            df.to_sql(
                name=table_name,
                con=con,
                if_exists="replace",
                index=False,
                schema="data_mart",
            )
    if args.upload_to_bigquery:
        dbcp.helpers.upload_schema_to_bigquery("data_mart")
