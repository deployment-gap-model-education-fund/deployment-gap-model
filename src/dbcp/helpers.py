"""Small helper functions for dbcp etl."""
import csv
import logging
import os
from io import StringIO
from pathlib import Path

import fsspec
import google.auth
import pandas as pd
import pandas_gbq
import sqlalchemy as sa
from google.cloud import bigquery
from tqdm import tqdm

import dbcp

logger = logging.getLogger(__name__)

SA_TO_BQ_TYPES = {
    "VARCHAR": "STRING",
    "INTEGER": "INTEGER",
    "FLOAT": "FLOAT",
    "BOOLEAN": "BOOL",
    "DATETIME": "DATETIME",
}
SA_TO_PD_TYPES = {
    "VARCHAR": "string",
    "INTEGER": "Int64",
    "FLOAT": "float64",
    "BOOLEAN": "boolean",
    "DATETIME": "datetime64[ns]",
}
SA_TO_BQ_MODES = {True: "NULLABLE", False: "REQUIRED"}


def get_schema_sql_alchemy_metadata(schema: str) -> sa.MetaData:
    """
    Get SQL Alchemy metadata object for a particular schema.

    Args:
        schema: the name of the database schema.
    Returns:
        metadata: the SQL alchemy metadata associated with the db schema.
    """
    if schema == "data_mart":
        return dbcp.metadata.data_mart.metadata
    elif schema == "data_warehouse":
        return dbcp.metadata.data_warehouse.metadata
    else:
        raise ValueError(f"{schema} is not a valid schema.")


def get_bq_schema_from_metadata(
    table_name: str, schema: str, dev: bool = True
) -> list[dict[str, str]]:
    """
    Create a BigQuery schema from SQL Alchemy metadata.

    Args:
        table_name: the name of the table.
        schema: the name of the database schema.
    Returns:
        bq_schema: a bigquery schema description.
    """
    table_name = f"{schema}.{table_name}"
    metadata = get_schema_sql_alchemy_metadata(schema)
    bq_schema = []
    for column in metadata.tables[table_name].columns:
        col_schema = {}
        col_schema["name"] = column.name
        col_schema["type"] = SA_TO_BQ_TYPES[str(column.type)]
        col_schema["mode"] = SA_TO_BQ_MODES[column.nullable]
        bq_schema.append(col_schema)
    return bq_schema


def enforce_dtypes(df: pd.DataFrame, table_name: str, schema: str):
    """Apply dtypes to a dataframe using the sqlalchemy metadata."""
    table_name = f"{schema}.{table_name}"
    metadata = get_schema_sql_alchemy_metadata(schema)
    try:
        table = metadata.tables[table_name]
    except KeyError:
        raise KeyError(f"{table_name} does not exist in metadata.")

    dtypes = {
        col.name: SA_TO_PD_TYPES[str(col.type)]
        for col in table.columns
        if col.name in df.columns
    }
    return df.astype(dtypes)


def get_sql_engine() -> sa.engine.Engine:
    """Create a sql alchemy engine from environment vars."""
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    return sa.create_engine(f"postgresql://{user}:{password}@{db}:5432")


def get_pudl_engine() -> sa.engine.Engine:
    """Create a sql alchemy engine for the pudl database."""
    pudl_data_path = get_pudl_resource("pudl.sqlite.gz")
    pudl_engine = sa.create_engine(f"sqlite:////{pudl_data_path}")
    return pudl_engine


def get_pudl_resource(pudl_resource: str, bucket: str) -> Path:
    """Given the name of a PUDL resource, return the path to the cached file.

    If the file is not cached, download it from S3 and return the path.

    Args:
        pudl_resource: The name of the PUDL resource to retrieve.
    Returns:
        pudl_resource_path: The path to the cached PUDL resource.
    """
    PUDL_VERSION = os.environ["PUDL_VERSION"]

    pudl_cache = Path("/app/data/data_cache/pudl/")
    pudl_cache.mkdir(exist_ok=True)
    pudl_version_cache = pudl_cache / PUDL_VERSION
    pudl_version_cache.mkdir(exist_ok=True)

    remote_pudl_resource_path = f"{bucket}/{PUDL_VERSION}/{pudl_resource}"
    local_pudl_resource_path = pudl_version_cache / pudl_resource

    fs = fsspec.filesystem(
        "filecache",
        target_protocol="gs",
        cache_storage=str(pudl_version_cache / "fsspec"),
    )
    file_size = fs.size(remote_pudl_resource_path)

    # open the remote_pudl_resource_path and track progress with tqdm
    with fs.open(remote_pudl_resource_path) as fo:
        with open(local_pudl_resource_path, "wb") as local_file:
            with tqdm(
                total=file_size, unit="B", unit_scale=True, unit_divisor=1024
            ) as pbar:
                while True:
                    buf = fo.read(8192)
                    if not buf:
                        break
                    local_file.write(buf)
                    pbar.update(len(buf))

    return local_pudl_resource_path


def track_tar_progress(members):
    """Use tqdm to track progress of tar extraction."""
    for member in tqdm(members):
        # this will be the current file being extracted
        yield member


def get_db_schema_tables(engine: sa.engine.Engine, schema: str) -> list[str]:
    """
    Get table names of database schema.

    Args:
        engine: sqlalchemy connection engine.
        schema: the name of the database schema.
    Return:
        table_names: the table names in the db schema.
    """
    inspector = sa.inspect(engine)
    table_names = inspector.get_table_names(schema=schema)

    if not table_names:
        raise ValueError(
            f"{schema} schema either doesn't exist or doesn't contain any tables. Try rerunning the etl and data mart pipelines."
        )

    return table_names


def upload_schema_to_bigquery(schema: str, dev: bool = True) -> None:
    """Upload a postgres schema to BigQuery."""
    logger.info("Loading tables to BigQuery.")

    # Get the schema table names
    engine = get_sql_engine()
    table_names = get_db_schema_tables(engine, schema)

    # read tables from dbcp schema in a dictionary of dfs
    loaded_tables = {}
    with engine.connect() as con:
        for table_name in table_names:
            loaded_tables[table_name] = pd.read_sql_table(
                table_name, con, schema=schema
            )
            loaded_tables[table_name] = enforce_dtypes(
                loaded_tables[table_name], table_name, schema
            )

    # load to big query
    credentials, project_id = google.auth.default()
    client = bigquery.Client(credentials=credentials, project=project_id)

    for table_name, df in loaded_tables.items():
        schema_environment = f"{schema}{'_dev' if dev else ''}"
        full_table_name = f"{schema_environment}.{table_name}"
        table_schema = get_bq_schema_from_metadata(table_name, schema, dev)
        logger.info(f"Loading: {table_name}")

        # Delete the table because pandas_gbq doesn't recreate the BQ
        # table schema which leads to problems when we change the metadata.
        table_id = f"{project_id}.{schema_environment}.{table_name}"
        client.delete_table(table_id, not_found_ok=True)

        pandas_gbq.to_gbq(
            df,
            full_table_name,
            project_id=project_id,
            if_exists="replace",
            credentials=credentials,
            table_schema=table_schema,
            chunksize=5000,
        )
        logger.info(f"Finished: {full_table_name}")


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
        if table.schema:
            table_name = f"{table.schema}.{table.name}"
        else:
            table_name = table.name

        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        cur.copy_expert(sql=sql, file=s_buf)
        dbapi_conn.commit()
