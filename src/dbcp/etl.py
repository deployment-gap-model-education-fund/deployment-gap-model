"""The ETL module create the data warehouse tables."""
import logging
from pathlib import Path
from typing import Dict

import pandas as pd

import dbcp
from dbcp.constants import OUTPUT_DIR
from dbcp.extract.ncsl_state_permitting import NCSLScraper
from dbcp.helpers import enforce_dtypes, psql_insert_copy
from dbcp.metadata.data_warehouse import metadata
from dbcp.transform.fips_tables import SPATIAL_CACHE
from dbcp.transform.helpers import GEOCODER_CACHE
from dbcp.validation.tests import validate_warehouse

logger = logging.getLogger(__name__)


def etl_eip_infrastructure() -> Dict[str, pd.DataFrame]:
    """EIP Infrastructure ETL."""
    # Extract
    source_path = Path("/app/data/raw/2023.05.24 OGW database.xlsx")
    eip_raw_dfs = dbcp.extract.eip_infrastructure.extract(source_path)

    # Transform
    eip_transformed_dfs = dbcp.transform.eip_infrastructure.transform(eip_raw_dfs)

    return eip_transformed_dfs


def etl_lbnl_iso_queue() -> Dict[str, pd.DataFrame]:
    """LBNL ISO Queues ETL."""
    lbnl_uri = "gs://dgm-archive/lbnl_iso_queue/queues_2023_clean_data.xlsx"
    lbnl_raw_dfs = dbcp.extract.lbnl_iso_queue.extract(lbnl_uri)
    lbnl_transformed_dfs = dbcp.transform.lbnl_iso_queue.transform(lbnl_raw_dfs)

    return lbnl_transformed_dfs


def etl_columbia_local_opp() -> Dict[str, pd.DataFrame]:
    """Columbia Local Opposition ETL."""
    # Extract
    source_path = Path(
        "/app/data/raw/2023.05.30 Opposition to Renewable Energy Facilities - FINAL.docx"
    )
    extractor = dbcp.extract.local_opposition.ColumbiaDocxParser()
    extractor.load_docx(source_path)
    docx_dfs = extractor.extract()

    # Transform
    transformed_dfs = dbcp.transform.local_opposition.transform(docx_dfs)

    return transformed_dfs


def etl_pudl_tables() -> Dict[str, pd.DataFrame]:
    """Pull tables from pudl sqlite database."""
    raw_pudl_tables = dbcp.extract.pudl.extract()
    return dbcp.transform.pudl.transform(raw_pudl_tables)


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
    census_uri = "gs://dgm-archive/census/tl_2021_us_county.zip"
    fips = dbcp.extract.fips_tables.extract_fips(census_uri)

    tribal_lands_uri = "gs://dgm-archive/census/tl_2021_us_aiannh.zip"
    fips["tribal_land"] = dbcp.extract.fips_tables.extract_census_tribal_land(
        tribal_lands_uri
    )

    out = dbcp.transform.fips_tables.transform(fips)

    return out


def etl_justice40() -> dict[str, pd.DataFrame]:
    """ETL white house environmental justice dataset."""
    source_path = Path("/app/data/raw/1.0-communities.csv")
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
    projects_path = (
        "gs://dgm-archive/synapse/offshore_wind/offshore_wind_projects_Q2_2024.csv"
    )
    locations_path = (
        "gs://dgm-archive/synapse/offshore_wind/offshore_wind_locations_Q2_2024.csv"
    )

    raw_offshore_dfs = dbcp.extract.offshore_wind.extract(
        locations_path=locations_path, projects_path=projects_path
    )
    offshore_transformed_dfs = dbcp.transform.offshore_wind.transform(raw_offshore_dfs)

    return offshore_transformed_dfs


def etl_protected_area_by_county() -> dict[str, pd.DataFrame]:
    """ETL the PAD-US intersection with TIGER county geometries."""
    source_path = Path("/app/data/raw/padus_intersect_counties.parquet")
    raw_df = dbcp.extract.protected_area_by_county.extract(source_path)
    transformed = dbcp.transform.protected_area_by_county.transform(raw_df)
    return transformed


def etl_energy_communities_by_county() -> dict[str, pd.DataFrame]:
    """ETL RMI's energy communities analysis."""
    source_path = Path("/app/data/raw/rmi_energy_communities_counties.parquet")
    raw_df = dbcp.extract.rmi_energy_communities.extract(source_path)
    transformed = dbcp.transform.rmi_energy_communities.transform(raw_df)
    return transformed


def etl_ballot_ready() -> dict[str, pd.DataFrame]:
    """ETL Ballot Ready election data."""
    source_uri = "gs://dgm-archive/ballot_ready/Climate Partners_Upcoming Races_All Tiers_20231013.csv"
    raw_df = dbcp.extract.ballot_ready.extract(source_uri)
    transformed = dbcp.transform.ballot_ready.transform(raw_df)
    return transformed


def etl_epa_avert() -> dict[str, pd.DataFrame]:
    """ETL EPA AVERT avoided emissions data."""
    # https://github.com/USEPA/AVERT/blob/v4.1.0/utilities/data/county-fips.txt
    path_county_region_xwalk = Path("/app/data/raw/avert_county-fips.txt")
    # https://www.epa.gov/avert/avoided-emission-rates-generated-avert
    path_emission_rates = Path("/app/data/raw/avert_emission_rates_04-25-23.xlsx")
    raw_dfs = dbcp.extract.epa_avert.extract(
        county_crosswalk_path=path_county_region_xwalk,
        emission_rates_path=path_emission_rates,
    )
    transformed = dbcp.transform.epa_avert.transform(raw_dfs)
    return transformed


def etl_gridstatus_isoqueues():
    """ETL gridstatus ISO queues."""
    raw_dfs = dbcp.extract.gridstatus_isoqueues.extract()
    transformed = dbcp.transform.gridstatus.transform(raw_dfs)
    return transformed


def etl_manual_ordinances() -> dict[str, pd.DataFrame]:
    """ETL manually maintained ordinances."""
    raw_dfs = dbcp.extract.manual_ordinances.extract()
    transformed = dbcp.transform.manual_ordinances.transform(raw_dfs)
    return transformed


def etl(args):
    """Run dbc ETL."""
    # Setup postgres
    engine = dbcp.helpers.get_sql_engine()
    with engine.connect() as con:
        engine.execute("CREATE SCHEMA IF NOT EXISTS data_warehouse")

    # Reduce size of caches if necessary
    GEOCODER_CACHE.reduce_size()
    SPATIAL_CACHE.reduce_size()

    etl_funcs = {
        "gridstatus": etl_gridstatus_isoqueues,
        "manual_ordinances": etl_manual_ordinances,
        "epa_avert": etl_epa_avert,
        "eip_infrastructure": etl_eip_infrastructure,
        "columbia_local_opp": etl_columbia_local_opp,
        "energy_communities_by_county": etl_energy_communities_by_county,
        "fips_tables": etl_fips_tables,
        "protected_area_by_county": etl_protected_area_by_county,
        "offshore_wind": etl_offshore_wind,
        "justice40_tracts": etl_justice40,
        "nrel_wind_solar_ordinances": etl_nrel_ordinances,
        "lbnl_iso_queue": etl_lbnl_iso_queue,
        "pudl": etl_pudl_tables,
        "ncsl_state_permitting": etl_ncsl_state_permitting,
        "ballot_ready": etl_ballot_ready,
    }

    # Extract and transform the data sets
    transformed_dfs = {}
    for dataset, etl_func in etl_funcs.items():
        logger.info(f"Processing: {dataset}")
        transformed_dfs.update(etl_func())

    # Delete any existing tables, and create them anew:
    metadata.drop_all(engine)
    metadata.create_all(engine)

    parquet_dir = OUTPUT_DIR / "data_warehouse"

    # Load table into postgres and parquet
    with engine.connect() as con:
        for table in metadata.sorted_tables:
            logger.info(f"Load {table.name} to postgres.")
            df = enforce_dtypes(
                transformed_dfs[table.name], table.name, "data_warehouse"
            )
            df = dbcp.helpers.trim_columns_length(df)
            df.to_sql(
                name=table.name,
                con=con,
                if_exists="append",
                index=False,
                schema="data_warehouse",
                chunksize=1000,
                method=psql_insert_copy,
            )
            df.to_parquet(parquet_dir / f"{table.name}.parquet", index=False)

    validate_warehouse(engine=engine)

    logger.info("Sucessfully finished ETL.")


if __name__ == "__main__":
    # debugging entry point
    etl(None)
    print("yay")
