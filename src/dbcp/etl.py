"""The ETL module create the data warehouse tables."""

import logging
from pathlib import Path
from typing import Callable, Dict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa

import dbcp
from dbcp.archivers.utils import ExtractionSettings
from dbcp.constants import OUTPUT_DIR
from dbcp.extract.fips_tables import CENSUS_URI, TRIBAL_LANDS_URI
from dbcp.extract.ncsl_state_permitting import NCSLScraper
from dbcp.helpers import enforce_dtypes, psql_insert_copy
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
    raw_pudl_tables = dbcp.extract.pudl_data.extract()
    return dbcp.transform.pudl_data.transform(raw_pudl_tables)


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
    fips = dbcp.extract.fips_tables.extract_fips(CENSUS_URI)

    fips["tribal_land"] = dbcp.extract.fips_tables.extract_census_tribal_land(
        TRIBAL_LANDS_URI
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
    # get the latest version of the offshore wind data from the candidate yaml file
    projects_uri = "airtable/Offshore Wind Locations Synapse Version/Projects.json"
    locations_uri = "airtable/Offshore Wind Locations Synapse Version/Locations.json"

    es = ExtractionSettings.from_yaml("/app/dbcp/settings.yaml")
    es.update_archive_generation_numbers()

    projects_uri = es.get_full_archive_uri(projects_uri)
    locations_uri = es.get_full_archive_uri(locations_uri)

    raw_offshore_dfs = dbcp.extract.offshore_wind.extract(
        locations_uri=locations_uri, projects_uri=projects_uri
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
    source_uri = "gs://dgm-archive/ballot_ready/climate_partners_upcoming_races_with_counties_20250204.csv"
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


def etl_acp_projects() -> dict[str, pd.DataFrame]:
    """ETL ACP projects."""
    acp_uri = "gs://dgm-archive/acp/projects_Q3_2024.csv"
    raw_dfs = dbcp.extract.acp_projects.extract(acp_uri)
    transformed = dbcp.transform.acp_projects.transform(raw_dfs)
    return transformed


def run_etl(funcs: dict[str, Callable], schema_name: str):
    """Execute etl functions and save outputs to parquet and postgres."""
    engine = dbcp.helpers.get_sql_engine()
    with engine.connect() as con:
        engine.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))

    transformed_dfs = {}
    for dataset, etl_func in funcs.items():
        logger.info(f"Processing: {dataset}")
        transformed_dfs.update(etl_func())

    # Delete any existing tables, and create them anew:
    metadata = dbcp.helpers.get_schema_sql_alchemy_metadata(schema_name)
    metadata.drop_all(engine)
    metadata.create_all(engine)

    parquet_dir = OUTPUT_DIR / schema_name
    parquet_dir.mkdir(exist_ok=True)

    # Load table into postgres and parquet
    with engine.connect() as con:
        for table in metadata.sorted_tables:
            logger.info(f"Load {table.name} to postgres.")
            df = enforce_dtypes(transformed_dfs[table.name], table.name, schema_name)
            df = dbcp.helpers.trim_columns_length(df)
            df.to_sql(
                name=table.name,
                con=con,
                if_exists="append",
                index=False,
                schema=schema_name,
                chunksize=1000,
                method=psql_insert_copy,
            )

            schema = dbcp.helpers.get_pyarrow_schema_from_metadata(
                table.name, schema_name
            )
            pa_table = pa.Table.from_pandas(df, schema=schema)
            pq.write_table(pa_table, parquet_dir / f"{table.name}.parquet")

    logger.info("Sucessfully finished ETL.")


def etl():
    """Run dbc ETL."""
    # Reduce size of caches if necessary
    GEOCODER_CACHE.reduce_size()
    SPATIAL_CACHE.reduce_size()

    # Run public ETL functions
    etl_funcs = {
        "offshore_wind": etl_offshore_wind,
        "gridstatus": etl_gridstatus_isoqueues,
        "manual_ordinances": etl_manual_ordinances,
        "epa_avert": etl_epa_avert,
        "eip_infrastructure": etl_eip_infrastructure,
        "columbia_local_opp": etl_columbia_local_opp,
        "energy_communities_by_county": etl_energy_communities_by_county,
        "fips_tables": etl_fips_tables,
        "protected_area_by_county": etl_protected_area_by_county,
        "justice40_tracts": etl_justice40,
        "nrel_wind_solar_ordinances": etl_nrel_ordinances,
        "lbnl_iso_queue": etl_lbnl_iso_queue,
        "pudl": etl_pudl_tables,
        "ncsl_state_permitting": etl_ncsl_state_permitting,
        "ballot_ready": etl_ballot_ready,
    }
    run_etl(etl_funcs, "data_warehouse")

    # Run private ETL functions
    etl_funcs = {
        "acp_projects": etl_acp_projects,
    }
    run_etl(etl_funcs, "private_data_warehouse")

    logger.info("Sucessfully finished ETL.")

    engine = dbcp.helpers.get_sql_engine()
    validate_warehouse(engine=engine)


if __name__ == "__main__":
    # debugging entry point
    etl()
    print("yay")
