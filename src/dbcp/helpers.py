"""Small helper functions for dbcp etl."""

import csv
import logging
import os
from datetime import UTC
from io import StringIO
from pathlib import Path
from typing import Literal

import addfips
import fsspec
import pandas as pd
import pyarrow as pa
import sqlalchemy as sa
from tqdm import tqdm
from upath import UPath

import dbcp
from dbcp.constants import DATA_DIR, DUCKDB_PATH
from dbcp.metadata import SchemaName

logger = logging.getLogger(__name__)

SA_TO_BQ_TYPES = {
    "VARCHAR": "STRING",
    "INTEGER": "INTEGER",
    "BIGINT": "INTEGER",
    "FLOAT": "FLOAT",
    "BOOLEAN": "BOOL",
    "DATETIME": "DATETIME",
}
SA_TO_PD_TYPES = {
    "VARCHAR": "string",
    "INTEGER": "Int64",
    "BIGINT": "Int64",
    "FLOAT": "Float64",
    "BOOLEAN": "boolean",
    "DATETIME": "datetime64[ns]",
}
SA_TO_PA_TYPES = {
    "VARCHAR": pa.string(),
    "INTEGER": pa.int64(),
    "BIGINT": pa.int64(),
    "FLOAT": pa.float64(),
    "BOOLEAN": pa.bool_(),
    "DATETIME": pa.timestamp("ms"),
}
SA_TO_BQ_MODES = {True: "NULLABLE", False: "REQUIRED"}


def get_schema_sql_alchemy_metadata(schema: SchemaName) -> sa.MetaData:
    """Get SQL Alchemy metadata object for a particular schema.

    Args:
        schema: the name of the database schema.

    Returns:
        metadata: the SQL alchemy metadata associated with the db schema.

    """
    if schema == SchemaName.DATA_MART:
        metadata = dbcp.metadata.data_mart.metadata
    if schema == SchemaName.DATA_WAREHOUSE:
        metadata = dbcp.metadata.data_warehouse.metadata
    return metadata


def get_bq_schema_from_metadata(
    table_name: str, schema: SchemaName
) -> list[dict[str, str]]:
    """Create a BigQuery schema from SQL Alchemy metadata.

    Args:
        table_name: the name of the table.
        schema: the name of the database schema.

    Returns:
        bq_schema: a bigquery schema description.

    """
    table_name = f"{schema.value}.{table_name}"
    metadata = get_schema_sql_alchemy_metadata(schema)
    bq_schema = []
    for column in metadata.tables[table_name].columns:
        col_schema = {}
        col_schema["name"] = column.name
        col_schema["type"] = SA_TO_BQ_TYPES[str(column.type)]
        col_schema["mode"] = SA_TO_BQ_MODES[column.nullable]
        bq_schema.append(col_schema)
    return bq_schema


def check_table_versions_equivalent(old_version: UPath, new_version: UPath) -> bool:
    """Takes path to two parquet files of the same table and return a bool indicating if there are differences.

    This function is meant to detect changes between deployments. It checks for major changes like
    new, missing, or changed rows. For float columns it compares within a reasonable tolerance.
    """
    old_df = pd.read_parquet(str(old_version))
    new_df = pd.read_parquet(str(new_version))

    try:
        pd.testing.assert_frame_equal(
            old_df,
            new_df,
            check_dtype=True,
            check_like=True,
            check_exact=False,
            rtol=1e-5,
            atol=1e-8,
        )
        return True
    except AssertionError:
        return False


def get_pyarrow_schema_from_metadata(table_name: str, schema: SchemaName) -> pa.Schema:
    """Create a PyArrow schema from SQL Alchemy metadata.

    Args:
        table_name: the name of the table.
        schema: the name of the database schema.

    Returns:
        pyarrow_schema: a PyArrow schema description.

    """
    table_name = f"{schema.value}.{table_name}"
    metadata = get_schema_sql_alchemy_metadata(schema)
    table_sa = metadata.tables[table_name]
    pyarrow_schema = []
    for column in table_sa.columns:
        pyarrow_schema.append((column.name, SA_TO_PA_TYPES[str(column.type)]))
    return pa.schema(pyarrow_schema)


def enforce_dtypes(df: pd.DataFrame, table_name: str, schema: SchemaName):
    """Apply dtypes to a dataframe using the sqlalchemy metadata."""
    table_name = f"{schema.value}.{table_name}"
    metadata = get_schema_sql_alchemy_metadata(schema)
    try:
        table = metadata.tables[table_name]
    except KeyError as e:
        raise KeyError(f"{table_name} does not exist in metadata.") from e

    for col in table.columns:
        # Add the column if it doesn't exist
        if col.name not in df.columns:
            df[col.name] = None
        if str(col.type) == "DATETIME":
            if not pd.api.types.is_datetime64_any_dtype(df[col.name]):
                df[col.name] = pd.to_datetime(df[col.name], errors="coerce")
            # drop the timezone in order to enable migration to Postgres.
            if (df[col.name].dt.tz is not None) and (df[col.name].dt.tz != UTC):
                logger.error(
                    f"Non-UTC timezone encountered in column {col.name} "
                    "while enforcing dtypes before postgres migration. "
                    f"Datetime values in {col.name} will be localized (timezone removed) "
                    "and UTC will be the assumed timezone. "
                    "Either convert to UTC with df[col.name].dt.tz_convert('UTC') "
                    "or add a timezone column to the table."
                )
            df[col.name] = df[col.name].dt.tz_localize(None)
        else:
            df[col.name] = df[col.name].astype(SA_TO_PD_TYPES[str(col.type)])

    # convert datetime[ns] columns to milliseconds
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].dt.floor("ms")
    return df


def get_postgres_engine(production: bool = False) -> sa.engine.Engine:
    """Create a sql alchemy engine from environment vars."""
    if not production:
        user = os.environ["STAGING_POSTGRES_USER"]
        password = os.environ["STAGING_POSTGRES_PASSWORD"]
        host = os.environ["STAGING_POSTGRES_HOST"]
        port = 6543
    else:
        user = os.environ["PROD_POSTGRES_USER"]
        password = os.environ["PROD_POSTGRES_PASSWORD"]
        host = os.environ["PROD_POSTGRES_HOST"]
        port = 5432
    return sa.create_engine(f"postgresql://{user}:{password}@{host}:{port}/postgres")


def get_duckdb_engine() -> sa.engine.Engine:
    """Return duckdb engine used for local storage when ETL runs."""
    DATA_DIR.mkdir(exist_ok=True)
    return sa.create_engine(f"duckdb:///{DUCKDB_PATH}")


def write_to_sql(
    df: pd.DataFrame,
    table_name: str,
    engine: sa.engine.Engine,
    schema_name: SchemaName,
    if_exists: Literal["fail", "replace", "append"] = "append",
    remote: bool = False,
):
    """Create data from a DataFrame to a postgres table.

    Args:
        df: DataFrame with table data.
        table_name: Name of table to create/append to in postgres.
        engine: sqlalchemy engine.
        schema_name: Name of schema like ``data_mart`` or ``data_warehouse``.
        if_exists: What to do if table already exists in postgres. See Pandas ``to_sql`` for options.
        remote: If writing to the production DB, everything goes in a single
            schema. Eventually, we will remove the data warehouse / mart distinction
            everywhere.

    """
    df = trim_columns_length(df)
    df = enforce_dtypes(df, table_name, schema_name)
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
        schema="catalyst" if remote else schema_name.value,
        chunksize=5000,  # adjust based on memory capacity
    )


def get_pudl_resource(
    pudl_resource: str, bucket: str = "s3://pudl.catalyst.coop/"
) -> Path:
    """Given the name of a PUDL resource, return the path to the cached file.

    If the file is not cached, download it from S3 and return the path.

    Args:
        pudl_resource: The name of the PUDL resource to retrieve.

    Returns:
        pudl_resource_path: The path to the cached PUDL resource.

    """
    try:
        file_paths = dbcp.extract.helpers.load_yml_file(DATA_DIR / "file_paths.yml")
        bucket = file_paths["pudl_data"].item()
    except Exception as e:
        logger.info(f"{e}: reverting to default input bucket.")
        bucket = bucket  # If failure, use default value of bucket provided.

    pudl_version = os.environ["PUDL_VERSION"]

    pudl_cache = DATA_DIR / "data_cache/pudl/"
    pudl_cache.mkdir(exist_ok=True)
    pudl_version_cache = pudl_cache / pudl_version
    pudl_version_cache.mkdir(exist_ok=True)

    remote_pudl_resource_path = f"{bucket}{pudl_version}/{pudl_resource}"
    local_pudl_resource_path = pudl_version_cache / pudl_resource

    if not local_pudl_resource_path.exists():
        fs = fsspec.filesystem("s3", anon=True)
        file_size = fs.size(remote_pudl_resource_path)

        # open the remote_pudl_resource_path and track progress with tqdm
        with (
            fs.open(remote_pudl_resource_path) as file,
            Path(local_pudl_resource_path).open("wb") as local_file,
            tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1024) as pbar,
        ):
            while True:
                buf = file.read(8192)
                if not buf:
                    break
                local_file.write(buf)
                pbar.update(len(buf))

    return local_pudl_resource_path


def track_tar_progress(members):
    """Use tqdm to track progress of tar extraction."""
    yield from tqdm(members)


def get_db_schema_tables(engine: sa.engine.Engine, schema: SchemaName) -> list[str]:
    """Get table names of database schema.

    Args:
        engine: sqlalchemy connection engine.
        schema: the name of the database schema.

    Return:
        table_names: the table names in the db schema.

    """
    inspector = sa.inspect(engine)
    table_names = inspector.get_table_names(schema=schema.value)

    if not table_names:
        raise ValueError(
            f"{schema} schema either doesn't exist or doesn't contain any tables. Try rerunning the etl and data mart pipelines."
        )

    return table_names


def psql_insert_copy(table, conn, keys, data_iter):
    """Insert data via COPY statement, which is much faster than INSERT.

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted

    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join([f'"{k}"' for k in keys])
        table_name = (
            f"{table.schema}.{table.name}" if table.schema is not None else table.name
        )

        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        cur.copy_expert(sql=sql, file=s_buf)
        dbapi_conn.commit()


def trim_columns_length(df: pd.DataFrame, length_limit: int = 63) -> pd.DataFrame:
    """Trim column length of a pandas dataframe to satisfy postgres column length limit."""
    df.columns = [col[:length_limit] for col in df.columns]
    return df


def add_fips_ids(
    df: pd.DataFrame,
    state_col: str = "state",
    county_col: str = "county",
    vintage: int = 2015,
) -> pd.DataFrame:
    """Add State and County FIPS IDs to a dataframe.

    To just add State FIPS IDs, make county_col = None.
    """
    # force the columns to be the nullable string types so we have a consistent
    # null value to filter out before feeding to addfips
    df = df.astype({state_col: pd.StringDtype()})
    if county_col:
        df = df.astype({county_col: pd.StringDtype()})
    af = addfips.AddFIPS(vintage=vintage)
    # Lookup the state and county FIPS IDs and add them to the dataframe:
    df["state_id_fips"] = df.apply(
        lambda x: (
            af.get_state_fips(state=x[state_col]) if pd.notnull(x[state_col]) else pd.NA
        ),
        axis=1,
    )

    # force the code columns to be nullable strings - the leading zeros are
    # important
    df = df.astype({"state_id_fips": pd.StringDtype()})

    logger.info(
        f"Assigned state FIPS codes for "
        f"{len(df[df.state_id_fips.notnull()]) / len(df):.2%} of records."
    )
    if county_col:
        df["county_id_fips"] = df.apply(
            lambda x: (
                af.get_county_fips(state=x[state_col], county=x[county_col])
                if pd.notnull(x[county_col]) and pd.notnull(x[state_col])
                else pd.NA
            ),
            axis=1,
        )
        # force the code columns to be nullable strings - the leading zeros are
        # important
        df = df.astype({"county_id_fips": pd.StringDtype()})
        logger.info(
            f"Assigned county FIPS codes for "
            f"{len(df[df.county_id_fips.notnull()]) / len(df):.2%} of records."
        )
    return df
