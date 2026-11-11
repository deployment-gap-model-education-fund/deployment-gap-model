"""Dagster definitions for the DBCP ETL."""

import dagster as dg

from dbcp import assets
from dbcp.resources import DuckDBResource

# DuckDB only allows one writer process, so run all steps in a single process.
data_warehouse_job = dg.define_asset_job(
    "data_warehouse_job",
    selection=dg.AssetSelection.groups("data_warehouse"),
    executor_def=dg.in_process_executor,
)
data_mart_job = dg.define_asset_job(
    "data_mart_job",
    selection=dg.AssetSelection.groups("data_mart"),
    executor_def=dg.in_process_executor,
)

defs = dg.Definitions(
    assets=dg.load_assets_from_modules([assets]),
    jobs=[data_warehouse_job, data_mart_job],
    resources={"duckdb": DuckDBResource()},
)
