"""DBC ETL logic."""
import logging
from pathlib import Path
from typing import Dict

import addfips
import pandas as pd
import pandas_gbq
import pydata_google_auth
import sqlalchemy as sa

import dbcp
from dbcp.constants import FIPS_CODE_VINTAGE, WORKING_PARTITIONS
from dbcp.extract.ncsl_state_permitting import NCSLScraper
from dbcp.schemas import TABLE_SCHEMAS
from dbcp.workspace.datastore import DBCPDatastore
from pudl.helpers import add_fips_ids as _add_fips_ids
from pudl.output.pudltabl import PudlTabl

logger = logging.getLogger(__name__)


def etl_eipinfrastructure() -> Dict[str, pd.DataFrame]:
    """EIP Infrastructure ETL."""
    # Extract
    ds = DBCPDatastore(sandbox=True, local_cache_path="/app/data/data_cache")
    eip_raw_dfs = dbcp.extract.eipinfrastructure.Extractor(ds).extract(
        update_date=WORKING_PARTITIONS["eipinfrastructure"]["update_date"])

    # Transform
    eip_transformed_dfs = dbcp.transform.eipinfrastructure.transform(
        eip_raw_dfs)

    return eip_transformed_dfs


def etl_lbnlisoqueues() -> Dict[str, pd.DataFrame]:
    """LBNL ISO Queues ETL."""
    # Extract
    ds = DBCPDatastore(sandbox=True, local_cache_path="/app/data/data_cache")
    lbnl_raw_dfs = dbcp.extract.lbnlisoqueues.Extractor(ds).extract(
        update_date=WORKING_PARTITIONS["lbnlisoqueues"]["update_date"])

    # Transform
    lbnl_transformed_dfs = dbcp.transform.lbnlisoqueues.transform(lbnl_raw_dfs)

    return lbnl_transformed_dfs


def etl_columbia_local_opp() -> Dict[str, pd.DataFrame]:
    """Columbia Local Opposition ETL."""
    # Extract
    source_path = Path('/app/data/raw/RELDI report updated 9.10.21 (1).docx')
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
    # add FIPS
    filled_location = mcoe.loc[:,['state', 'county']].fillna('')
    fips = _add_fips_ids(filled_location, vintage=FIPS_CODE_VINTAGE)
    mcoe = pd.concat([mcoe, fips[['state_id_fips', 'county_id_fips']]], axis=1, copy=False)
    mcoe = TABLE_SCHEMAS["mcoe"].validate(mcoe)
    pudl_tables["mcoe"] = mcoe

    return pudl_tables


def etl_ncsl_state_permitting() -> Dict[str, pd.DataFrame]:
    """NCSL State Permitting for Wind ETL."""
    source_path = Path('/app/data/raw/ncsl_state_permitting_wind.csv')
    if not source_path.exists():
        NCSLScraper().scrape_and_save_to_disk(source_path)
    raw_df = dbcp.extract.ncsl_state_permitting.extract(source_path)

    out = dbcp.transform.ncsl_state_permitting.transform(raw_df)

    return out


def etl_master_fips_table() -> Dict[str, pd.DataFrame]:
    """Master state and county FIPS table ETL."""
    af = addfips.AddFIPS()
    county_dict = af._counties
    state_dict = af._states
    state_df = pd.DataFrame(state_dict.items(), columns=['state', 'state_id_fips'])
    # for now keep only the two letter state abbreviations
    state_df = state_df[state_df.state.str.len() == 2].reset_index(drop=True)

    county_df = pd.concat([pd.DataFrame({'state_id_fips': item[0],
                          'county': item[1].keys(),
                          'county_id_fips': item[1].values()})
            for item in county_dict.items()], axis=0)
    # this county_pattern is taken directly from addfips
    # going to change this in the future/maybe just absorb addfips
    county_pattern = r" (county|city|city and borough|borough|census area|municipio|municipality|district|parish)$"
    county_df['county'] = county_df['county'].str.replace(county_pattern, '', regex=True)
    county_df['county'] = county_df['county'].str.replace('st.', 'saint', regex=False)
    county_df = county_df.drop_duplicates()
    county_df['county_id_fips'] = county_df['state_id_fips'] + county_df['county_id_fips']
    county_df = county_df.join(state_df.set_index('state_id_fips'), on='state_id_fips').reset_index(drop=True)
    county_df = county_df[['state_id_fips', 'county_id_fips', 'state', 'county']]

    return {'state_county_fips_table': county_df}


def etl(args):
    """Run dbc ETL."""
    # Setup postgres
    engine = dbcp.helpers.get_sql_engine()
    with engine.connect() as con:
        engine.execute("CREATE SCHEMA IF NOT EXISTS dbcp")

    etl_funcs = {
        "eipinfrastructure": etl_eipinfrastructure,
        "lbnlisoqueues": etl_lbnlisoqueues,
        "pudl": etl_pudl_tables,
        "ncsl_state_permitting": etl_ncsl_state_permitting,
        "columbia_local_opp": etl_columbia_local_opp,
        "master_fips_table": etl_master_fips_table
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
        output_path = Path("/app/data/output/")
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
                if TABLE_SCHEMAS.get(table_name):
                    loaded_tables[table_name] = TABLE_SCHEMAS[table_name].validate(
                        table)
                else:
                    loaded_tables[table_name] = table

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
