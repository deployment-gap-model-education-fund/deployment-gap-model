"""Dagster resources."""

import logging

import dagster as dg
import pandas as pd
import sqlalchemy as sa

from dbcp.helpers import (
    get_duckdb_engine,
    get_schema_sql_alchemy_metadata,
    write_to_sql,
)
from dbcp.metadata import SchemaName

logger = logging.getLogger(__name__)


class DuckDBResource(dg.ConfigurableResource):
    """Local duckdb database that holds the data warehouse and data mart tables."""

    def get_engine(self) -> sa.engine.Engine:
        """Return a SQLAlchemy engine for the duckdb database."""
        return get_duckdb_engine()

    def write_tables(
        self, dfs: dict[str, pd.DataFrame], schema_name: SchemaName
    ) -> dg.MaterializeResult:
        """Drop, recreate, and load tables into a duckdb schema."""
        engine = self.get_engine()
        with engine.begin() as con:
            con.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema_name.value}"))

        metadata = get_schema_sql_alchemy_metadata(schema_name)
        tables = [metadata.tables[f"{schema_name.value}.{name}"] for name in dfs]
        metadata.drop_all(engine, tables=tables)
        metadata.create_all(engine, tables=tables)

        # sorted_tables loads parents before children (foreign keys)
        for table in metadata.sorted_tables:
            if table in tables:
                logger.info(f"Load {table.name} to duckdb.")
                write_to_sql(
                    df=dfs[table.name],
                    table_name=table.name,
                    engine=engine,
                    schema_name=schema_name,
                    if_exists="append",
                )
        return dg.MaterializeResult(
            metadata={"num_rows": {name: len(df) for name, df in dfs.items()}}
        )
