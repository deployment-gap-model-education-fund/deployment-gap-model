"""The ETL module create the data warehouse tables."""
import logging
from pathlib import Path
from typing import Dict

import pandas as pd
import sqlalchemy as sa

import dbcp
from dbcp.constants import FIPS_CODE_VINTAGE, WORKING_PARTITIONS
from dbcp.extract.ncsl_state_permitting import NCSLScraper
from dbcp.schemas import TABLE_SCHEMAS
from dbcp.transform.helpers import GEOCODER_CACHE
from dbcp.workspace.datastore import DBCPDatastore
from pudl.helpers import add_fips_ids as _add_fips_ids
from pudl.output.pudltabl import PudlTabl

logger = logging.getLogger(__name__)


def etl_eip_infrastructure() -> Dict[str, pd.DataFrame]:
    """EIP Infrastructure ETL."""
    # Extract
    source_path = Path("/app/data/raw/2022.03.22OGW.xlsx")
    eip_raw_dfs = dbcp.extract.eip_infrastructure.extract(source_path)

    # Transform
    eip_transformed_dfs = dbcp.transform.eip_infrastructure.transform(eip_raw_dfs)

    return eip_transformed_dfs


def etl_lbnlisoqueues() -> Dict[str, pd.DataFrame]:
    """LBNL ISO Queues ETL."""
    # Extract
    ds = DBCPDatastore(sandbox=True, local_cache_path="/app/data/data_cache")
    lbnl_raw_dfs = dbcp.extract.lbnlisoqueues.Extractor(ds).extract(
        update_date=WORKING_PARTITIONS["lbnlisoqueues"]["update_date"]
    )

    # Transform
    lbnl_transformed_dfs = dbcp.transform.lbnlisoqueues.transform(lbnl_raw_dfs)

    return lbnl_transformed_dfs


def etl_columbia_local_opp() -> Dict[str, pd.DataFrame]:
    """Columbia Local Opposition ETL."""
    # Extract
    source_path = Path("/app/data/raw/RELDI report updated 9.10.21 (1).docx")
    extractor = dbcp.extract.local_opposition.ColumbiaDocxParser()
    extractor.load_docx(source_path)
    raw_dfs = extractor.extract()

    # Transform
    transformed_dfs = dbcp.transform.local_opposition.transform(raw_dfs)

    return transformed_dfs


def etl_pudl_tables() -> Dict[str, pd.DataFrame]:
    """Pull tables from pudl sqlite database."""
    pudl_data_path = dbcp.helpers.download_pudl_data()

    pudl_tables = {}

    pudl_engine = sa.create_engine(
        f"sqlite:////{pudl_data_path}/pudl_data/sqlite/pudl.sqlite"
    )
    pudl_out = PudlTabl(
        pudl_engine,
        start_date="2020-01-01",
        end_date="2020-12-31",
        freq="AS",
        fill_fuel_cost=False,
        roll_fuel_cost=True,
        fill_net_gen=True,
    )

    mcoe = pudl_out.mcoe(all_gens=True)
    # add FIPS
    filled_location = mcoe.loc[:, ["state", "county"]].fillna("")
    fips = _add_fips_ids(filled_location, vintage=FIPS_CODE_VINTAGE)
    mcoe = pd.concat(
        [mcoe, fips[["state_id_fips", "county_id_fips"]]], axis=1, copy=False
    )
    mcoe = TABLE_SCHEMAS["mcoe"].validate(mcoe)
    pudl_tables["mcoe"] = mcoe

    return pudl_tables


def etl_ncsl_state_permitting() -> Dict[str, pd.DataFrame]:
    """NCSL State Permitting for Wind ETL."""
    source_path = Path("/app/data/raw/ncsl_state_permitting_wind.csv")
    if not source_path.exists():
        NCSLScraper().scrape_and_save_to_disk(source_path)
    raw_df = dbcp.extract.ncsl_state_permitting.extract(source_path)

    out = dbcp.transform.ncsl_state_permitting.transform(raw_df)

    return out


def etl_fips_tables() -> Dict[str, pd.DataFrame]:
    """Master state and county FIPS table ETL."""
    fips = dbcp.extract.fips_tables.extract(vintage=FIPS_CODE_VINTAGE)
    out = dbcp.transform.fips_tables.transform(fips)

    return out


def etl(args):
    """Run dbc ETL."""
    # Setup postgres
    engine = dbcp.helpers.get_sql_engine()
    with engine.connect() as con:
        engine.execute("CREATE SCHEMA IF NOT EXISTS data_warehouse")

    # Reduce size of geocoder cache if necessary
    GEOCODER_CACHE.reduce_size()

    etl_funcs = {
        "eip_infrastructure": etl_eip_infrastructure,
        "lbnlisoqueues": etl_lbnlisoqueues,
        "pudl": etl_pudl_tables,
        "ncsl_state_permitting": etl_ncsl_state_permitting,
        "columbia_local_opp": etl_columbia_local_opp,
        "fips_tables": etl_fips_tables,
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
            df.to_sql(
                name=table_name,
                con=con,
                if_exists="replace",
                index=False,
                schema="data_warehouse",
            )

    # TODO: Writing to CSVs is a temporary solution for getting data into Tableau
    # This should be removed once we have cloudsql setup.
    if args.csv:
        logger.info("Writing tables to CSVs.")
        output_path = Path("/app/data/output/")
        for table_name, df in transformed_dfs.items():
            df.to_csv(output_path / f"{table_name}.csv", index=False)

    if args.upload_to_bigquery:
        dbcp.helpers.upload_schema_to_bigquery("data_warehouse")

    logger.info("Sucessfully finished ETL.")
