"""The ETL module create the data warehouse tables."""

import logging
from typing import Callable, Dict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa

import dbcp
from dbcp.archivers.utils import ExtractionSettings
from dbcp.constants import DATA_DIR, OUTPUT_DIR
from dbcp.extract.ballot_ready import BR_URI
from dbcp.extract.fips_tables import CENSUS_URI, TRIBAL_LANDS_URI
from dbcp.extract.ncsl_state_permitting import NCSLScraper
from dbcp.helpers import write_to_postgres
from dbcp.transform.fips_tables import SPATIAL_CACHE
from dbcp.transform.helpers import GEOCODER_CACHES
from dbcp.validation.tests import validate_data_mart, validate_warehouse

logger = logging.getLogger(__name__)


def etl_eip_infrastructure() -> Dict[str, pd.DataFrame]:
    """EIP Infrastructure ETL."""
    # Extract
    eip_raw_dfs = dbcp.extract.eip_infrastructure.extract()

    # Transform
    eip_transformed_dfs = dbcp.transform.eip_infrastructure.transform(eip_raw_dfs)

    return eip_transformed_dfs


def etl_lbnl_iso_queue() -> Dict[str, pd.DataFrame]:
    """LBNL ISO Queues ETL."""
    lbnl_uri = "gs://dgm-archive/lbnl_iso_queue/queues_2024_clean_data.xlsx"
    lbnl_raw_dfs = dbcp.extract.lbnl_iso_queue.extract(lbnl_uri)
    lbnl_transformed_dfs = dbcp.transform.lbnl_iso_queue.transform(lbnl_raw_dfs)

    return lbnl_transformed_dfs


def etl_fyi_queue() -> Dict[str, pd.DataFrame]:
    """Interconnection.fyi ISO Queues ETL."""
    fyi_uri = "gs://dgm-archive/interconnection.fyi/interconnection_fyi_dataset_2026-02-01.csv"
    fyi_raw_dfs = dbcp.extract.fyi_queue.extract(fyi_uri)
    fyi_transformed_dfs = dbcp.transform.fyi_queue.transform(fyi_raw_dfs)
    return fyi_transformed_dfs


def etl_columbia_local_opp() -> Dict[str, pd.DataFrame]:
    """Columbia Local Opposition ETL."""
    # Extract
    source_path = (
        DATA_DIR
        / "raw/2023.05.30 Opposition to Renewable Energy Facilities - FINAL.docx"
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
    source_path = DATA_DIR / "raw/ncsl_state_permitting_wind.csv"
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
    source_path = DATA_DIR / "raw/1.0-communities.csv"
    raw = dbcp.extract.justice40.extract(source_path)
    out = dbcp.transform.justice40.transform(raw)
    return out


def etl_nrel_ordinances() -> dict[str, pd.DataFrame]:
    """ETL NREL state and local ordinances for wind and solar."""
    wind_source_path = DATA_DIR / "raw/NREL_Wind_Ordinances.xlsx"
    solar_source_path = DATA_DIR / "raw/NREL_Solar_Ordinances.xlsx"
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
    projects_uri = "airtable/Offshore Wind Locations DBCP Version/Projects.json"
    locations_uri = "airtable/Offshore Wind Locations DBCP Version/Locations.json"

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
    source_path = DATA_DIR / "raw/padus_intersect_counties.parquet"
    raw_df = dbcp.extract.protected_area_by_county.extract(source_path)
    transformed = dbcp.transform.protected_area_by_county.transform(raw_df)
    return transformed


def etl_energy_communities_by_county() -> dict[str, pd.DataFrame]:
    """ETL RMI's energy communities analysis."""
    source_path = DATA_DIR / "raw/rmi_energy_communities_counties.parquet"
    raw_df = dbcp.extract.rmi_energy_communities.extract(source_path)
    transformed = dbcp.transform.rmi_energy_communities.transform(raw_df)
    return transformed


def etl_ballot_ready() -> dict[str, pd.DataFrame]:
    """ETL Ballot Ready election data."""
    source_uri = BR_URI
    raw_df = dbcp.extract.ballot_ready.extract(source_uri)
    transformed = dbcp.transform.ballot_ready.transform(raw_df)
    return transformed


def etl_epa_avert() -> dict[str, pd.DataFrame]:
    """ETL EPA AVERT avoided emissions data."""
    # https://github.com/USEPA/AVERT/blob/v4.1.0/utilities/data/county-fips.txt
    path_county_region_xwalk = DATA_DIR / "raw/avert_county-fips.txt"
    # https://www.epa.gov/avert/avoided-emission-rates-generated-avert
    path_emission_rates = DATA_DIR / "raw/avert_emission_rates_04-25-23.xlsx"
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
    raw_dfs = dbcp.extract.acp_projects.extract()
    transformed = dbcp.transform.acp_projects.transform(raw_dfs)
    return transformed


def write_to_postgres_and_parquet(
    dfs: dict[str, pd.DataFrame], engine: sa.engine.Engine, schema_name: str
):
    """Write data mart tables from a schema to postgres and parquet."""
    # Ensure schema exists in a committed transaction
    with engine.begin() as con:
        con.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))

    # Delete any existing tables, and create them anew
    metadata = dbcp.helpers.get_schema_sql_alchemy_metadata(schema_name)
    table_names = dfs.keys()
    tables = [metadata.tables[schema_name + "." + name] for name in table_names]
    # There are foreign key dependencies between the data warehouse and
    # private data warehouse (and data_mart and private_data_mart),
    # so they share the same metadata respectively. Thus, only drop
    # a subset of tables when creating the private data warehouse.
    if (schema_name == "data_warehouse") or (schema_name == "data_mart"):
        metadata.drop_all(engine)
    else:
        metadata.drop_all(engine, tables=tables)
    metadata.create_all(engine, tables=tables)

    parquet_dir = OUTPUT_DIR / f"{schema_name}"
    parquet_dir.mkdir(exist_ok=True)

    # Load table into postgres and parquet
    for table in metadata.sorted_tables:
        if table in tables:
            logger.info(f"Load {table.name} to postgres.")
            df = write_to_postgres(
                df=dfs[table.name],
                table_name=table.name,
                engine=engine,
                schema_name=schema_name,
                if_exists="append",
            )
            schema = dbcp.helpers.get_pyarrow_schema_from_metadata(
                table.name, schema_name
            )
            pa_table = pa.Table.from_pandas(df, schema=schema)
            pq.write_table(pa_table, parquet_dir / f"{table.name}.parquet")


def run_etl(funcs: dict[str, Callable], schema_name: str):
    """Execute etl functions and save outputs to parquet and postgres."""
    engine = dbcp.helpers.get_sql_engine()

    transformed_dfs = {}
    for dataset, etl_func in funcs.items():
        logger.info(f"Processing: {dataset}")
        transformed_dfs.update(etl_func())

    write_to_postgres_and_parquet(
        dfs=transformed_dfs, engine=engine, schema_name=schema_name
    )

    logger.info(f"Sucessfully finished {schema_name} ETL.")


def create_data_warehouse():
    """Create data warehouse tables by ETL-ing each data source."""
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
        "fyi_queue": etl_fyi_queue,
    }
    run_etl(etl_funcs, "private_data_warehouse")


def create_data_mart(engine, private_only: bool = False):  # noqa: C901
    """Create data mart tables by calling create_data_mart in each data mart module."""
    modules = dbcp.data_mart.get_data_mart_modules()
    data_mart_tables: dict[str, pd.DataFrame] = {}
    private_table_names: set[str] = set()
    for module, module_info in modules:
        module_private = set(getattr(module, "PRIVATE_DATA_MART_TABLES", set()))
        # If we're creating the private data mart only
        # and this module doesn't have any private tables
        # in it, skip it.
        if private_only and not module_private:
            continue
        try:
            data = module.create_data_mart(engine=engine)
        except AttributeError:
            raise AttributeError(
                f"{module_info.name} has no attribute 'create_data_mart'."
                "Make sure the data mart module implements create_data_mart function."
            )
        if isinstance(data, pd.DataFrame):
            name = module_info.name
            # If we're creating the private data mart only
            # and this specific table is not private, skip it.
            if private_only and name not in module_private:
                continue
            assert (
                name not in data_mart_tables.keys()
            ), f"Key {name} already exists in data mart"
            data_mart_tables[name] = data
            if name in module_private:
                private_table_names.add(name)
        elif isinstance(data, dict):
            # if only creating private tables, drop non private tables right away
            if private_only:
                data = {k: v for k, v in data.items() if k in module_private}
            # if no private tables are left from this module, skip
            if not data:
                continue
            assert (
                len([key for key in data.keys() if key in data_mart_tables.keys()]) == 0
            ), f"Dict key from {module_info.name} already exists"
            data_mart_tables.update(data)
            private_table_names.update(module_private.intersection(data.keys()))
        else:
            raise TypeError(
                f"Expecting pd.DataFrame or dict of dataframes. Got {type(data)}"
            )

    data_mart_dfs = {
        name: df
        for name, df in data_mart_tables.items()
        if name not in private_table_names
    }
    private_data_mart_dfs = {
        name: df for name, df in data_mart_tables.items() if name in private_table_names
    }
    if private_only:
        write_to_postgres_and_parquet(
            dfs=private_data_mart_dfs,
            engine=engine,
            schema_name="private_data_mart",
        )
    else:
        for schema_name in ["data_mart", "private_data_mart"]:
            if schema_name == "data_mart":
                write_to_postgres_and_parquet(
                    dfs=data_mart_dfs, engine=engine, schema_name=schema_name
                )
            else:
                write_to_postgres_and_parquet(
                    dfs=private_data_mart_dfs, engine=engine, schema_name=schema_name
                )


def etl(schema="all"):
    """Run dbc ETL."""
    # Reduce size of caches if necessary
    GEOCODER_CACHES.reduce_cache_sizes()
    SPATIAL_CACHE.reduce_size()
    engine = dbcp.helpers.get_sql_engine()

    # Run public ETL functions
    if (schema == "data_warehouse") or (schema == "all"):
        create_data_warehouse()
        validate_warehouse(engine=engine)
    if (schema == "data_mart") or (schema == "all"):
        create_data_mart(engine=engine)
        validate_data_mart(engine=engine)
    if schema == "private_data_mart":
        create_data_mart(engine=engine, private_only=True)

    logger.info("Sucessfully finished ETL.")


if __name__ == "__main__":
    # debugging entry point
    etl()
    print("yay")
