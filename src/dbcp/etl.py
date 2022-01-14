"""DBC ETL logic."""
import logging
from pathlib import Path
from typing import Dict

import pandas as pd
import pandas_gbq
import pydata_google_auth
import sqlalchemy as sa

import dbcp
from dbcp.constants import WORKING_PARTITIONS
from dbcp.schemas import TABLE_SCHEMAS
from dbcp.workspace.datastore import DBCPDatastore
from pudl.output.pudltabl import PudlTabl

logger = logging.getLogger(__name__)


def etl_eipinfrastructure() -> Dict[str, pd.DataFrame]:
    """EIP Infrastructure ETL."""
    # Extract
    ds = DBCPDatastore(sandbox=True, local_cache_path="/app/input")
    eip_raw_dfs = dbcp.extract.eipinfrastructure.Extractor(ds).extract(
        update_date=WORKING_PARTITIONS["eipinfrastructure"]["update_date"])

    # Transform
    eip_transformed_dfs = dbcp.transform.eipinfrastructure.transform(eip_raw_dfs)

    return eip_transformed_dfs


def etl_lbnlisoqueues() -> Dict[str, pd.DataFrame]:
    """LBNL ISO Queues ETL."""
    # Extract
    ds = DBCPDatastore(sandbox=True, local_cache_path="/app/input")
    lbnl_raw_dfs = dbcp.extract.lbnlisoqueues.Extractor(ds).extract(
        update_date=WORKING_PARTITIONS["lbnlisoqueues"]["update_date"])

    # Transform
    lbnl_transformed_dfs = dbcp.transform.lbnlisoqueues.transform(lbnl_raw_dfs)

    return lbnl_transformed_dfs


def etl_pudl_tables() -> Dict[str, pd.DataFrame]:
    """Pull tables from pudl sqlite database."""
    pudl_data_path = dbcp.helpers.download_pudl_data()

    pudl_tables = {}

    pudl_engine = sa.create_engine(
        f"sqlite:////{pudl_data_path}/pudl_data/sqlite/pudl.sqlite")
    pudl_out = PudlTabl(
        pudl_engine,
        start_date='2020-01-01',
        end_date='2020-12-31',
        freq='AS',
        fill_fuel_cost=True,
        roll_fuel_cost=True,
        fill_net_gen=True,
    )

    mcoe = pudl_out.mcoe(all_gens=True)
    mcoe = TABLE_SCHEMAS["mcoe"].validate(mcoe)
    pudl_tables["mcoe"] = mcoe

    return pudl_tables


def etl(args):
    """Run dbc ETL."""
    # Setup postgres
    engine = dbcp.helpers.get_sql_engine()
    with engine.connect() as con:
        engine.execute("CREATE SCHEMA IF NOT EXISTS dbcp")

    etl_funcs = {
        "eipinfrastructure": etl_eipinfrastructure,
        "lbnlisoqueues": etl_lbnlisoqueues,
        "pudl": etl_pudl_tables
    }

    # Extract and transform the data sets
    transformed_dfs = {}
    for dataset, etl_func in etl_funcs.items():
        logger.info(f"Processing: {dataset}")
        transformed_dfs.update(etl_func())

    # Load table into postgres
    with engine.connect() as con:
        for table_name, df in transformed_dfs.items():
            logger.info(f"Load {table_name} to postgres.")
            df.to_sql(name=table_name, con=con, if_exists="replace",
                      index=False, schema="dbcp")

    # TODO: Writing to CSVs is a temporary solution for getting data into Tableau
    # This should be removed once we have cloudsql setup.
    if args.csv:
        logger.info('Writing tables to CSVs.')
        output_path = Path("/app/output/")
        for table_name, df in transformed_dfs.items():
            df.to_csv(output_path / f"{table_name}.csv", index=False)

    if args.upload_to_bigquery:
        logger.info('Loading tables to BigQuery.')

        # read tables from dbcp schema in a dictionary of dfs
        loaded_tables = {}
        with engine.connect() as con:
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'dbcp';"
            table_names = list(pd.read_sql(query, con)["table_name"])

            for table_name in table_names:
                table = pd.read_sql_table(table_name, con, schema="dbcp")
                # Validate the schemas again
                loaded_tables[table_name] = TABLE_SCHEMAS[table_name].validate(table)

        # load to big query
        SCOPES = [
            'https://www.googleapis.com/auth/cloud-platform',
        ]

        credentials = pydata_google_auth.get_user_credentials(
            SCOPES)

        for table_name, df in loaded_tables.items():
            logger.info(f"Loading: {table_name}")
            pandas_gbq.to_gbq(
                df, f"dbcp_data.{table_name}", project_id="dbcp-dev", if_exists="replace", credentials=credentials)
            logger.info(f"Finished: {table_name}")

    logger.info("Sucessfully finished ETL.")
