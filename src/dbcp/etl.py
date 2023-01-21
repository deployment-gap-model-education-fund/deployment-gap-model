"""The ETL module create the data warehouse tables."""
import logging
from pathlib import Path
from typing import Dict

import pandas as pd
import sqlalchemy as sa

import dbcp
from dbcp.constants import FIPS_CODE_VINTAGE
from dbcp.extract.ncsl_state_permitting import NCSLScraper
from dbcp.metadata.data_warehouse import metadata
from dbcp.transform.helpers import GEOCODER_CACHE
from pudl.helpers import add_fips_ids as _add_fips_ids
from pudl.output.pudltabl import PudlTabl

logger = logging.getLogger(__name__)


def etl_eip_infrastructure() -> Dict[str, pd.DataFrame]:
    """EIP Infrastructure ETL."""
    # Extract
    source_path = Path("/app/data/raw/fossil_infrastructure.xlsx")
    eip_raw_dfs = dbcp.extract.eip_infrastructure.extract(source_path)

    # Transform
    eip_transformed_dfs = dbcp.transform.eip_infrastructure.transform(eip_raw_dfs)

    return eip_transformed_dfs


def etl_lbnl_iso_queue_2021() -> Dict[str, pd.DataFrame]:
    """LBNL ISO Queues 2021 ETL."""
    source_path = Path("/app/data/raw/queues_2021_clean_data.xlsx")
    lbnl_raw_dfs = dbcp.extract.lbnl_iso_queue_2021.extract(source_path)
    lbnl_transformed_dfs = dbcp.transform.lbnl_iso_queue_2021.transform(lbnl_raw_dfs)

    return lbnl_transformed_dfs


def etl_columbia_local_opp() -> Dict[str, pd.DataFrame]:
    """Columbia Local Opposition ETL."""
    # Extract
    source_path = Path("/app/data/raw/RELDI report updated 9.10.21 (1).docx")
    extractor = dbcp.extract.local_opposition.ColumbiaDocxParser()
    extractor.load_docx(source_path)
    docx_dfs = extractor.extract()

    source_path_update = Path("./data/raw/RELDI_local_opposition_2022-03-24.csv")
    update_dfs = dbcp.extract.local_opposition._extract_march_2022_update(
        source_path_update
    )

    # Transform
    combined = dbcp.transform.local_opposition._combine_updates(docx_dfs, update_dfs)
    transformed_dfs = dbcp.transform.local_opposition.transform(combined)

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
    mcoe = mcoe.convert_dtypes()
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


def etl_justice40() -> dict[str, pd.DataFrame]:
    """ETL white house environmental justice dataset."""
    source_path = Path("/app/data/raw/communities-2022-05-31-1915GMT.zip")
    raw = dbcp.extract.justice40.extract(source_path)
    out = dbcp.transform.justice40.transform(raw)
    return out


def etl_nrel_ordinances() -> dict[str, pd.DataFrame]:
    """ETL NREL state and local ordinances for wind and solar."""
    wind_source_path = Path("/app/data/raw/NREL_Wind_Ordinances.xlsx")
    solar_source_path = Path("/app/data/raw/NREL_Solar_Ordinances.xlsx")
    wind_raw_dfs = dbcp.extract.nrel_wind_solar_ordinances.extract(
        wind_source_path, wind_or_solar="wind"
    )
    solar_raw_dfs = dbcp.extract.nrel_wind_solar_ordinances.extract(
        solar_source_path, wind_or_solar="solar"
    )
    nrel_raw_dfs = wind_raw_dfs | solar_raw_dfs

    nrel_transformed_dfs = dbcp.transform.nrel_wind_solar_ordinances.transform(
        nrel_raw_dfs
    )

    return nrel_transformed_dfs


def etl_offshore_wind() -> dict[str, pd.DataFrame]:
    """ETL manually curated offshore wind data."""
    locations_path = Path("/app/data/raw/offshore_wind_locations.csv")
    projects_path = Path("/app/data/raw/offshore_wind_projects.csv")
    raw_offshore_dfs = dbcp.extract.offshore_wind.extract(
        locations_path=locations_path, projects_path=projects_path
    )
    offshore_transformed_dfs = dbcp.transform.offshore_wind.transform(raw_offshore_dfs)

    return offshore_transformed_dfs


def etl(args):
    """Run dbc ETL."""
    # Setup postgres
    engine = dbcp.helpers.get_sql_engine()
    with engine.connect() as con:
        engine.execute("CREATE SCHEMA IF NOT EXISTS data_warehouse")

    # Reduce size of geocoder cache if necessary
    GEOCODER_CACHE.reduce_size()

    etl_funcs = {
        "offshore_wind": etl_offshore_wind,
        "justice40_tracts": etl_justice40,
        "nrel_wind_solar_ordinances": etl_nrel_ordinances,
        "eip_infrastructure": etl_eip_infrastructure,
        "lbnl_iso_queue_2021": etl_lbnl_iso_queue_2021,
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

    # Delete any existing tables, and create them anew:
    metadata.drop_all(engine)
    metadata.create_all(engine)

    # Load table into postgres
    with engine.connect() as con:
        for table in metadata.sorted_tables:
            logger.info(f"Load {table.name} to postgres.")
            transformed_dfs[table.name].to_sql(
                name=table.name,
                con=con,
                if_exists="append",
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
