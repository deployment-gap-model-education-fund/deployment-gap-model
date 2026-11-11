"""Dagster assets that ETL each data source into the duckdb warehouse and marts."""

import importlib.resources
import logging

import dagster as dg
import pandas as pd

import dbcp
from dbcp.archivers.utils import ExtractionSettings
from dbcp.constants import DATA_DIR
from dbcp.data_mart import get_data_mart_modules
from dbcp.extract.civis import extract as extract_civis
from dbcp.extract.helpers import load_yml_file
from dbcp.metadata import SchemaName
from dbcp.resources import DuckDBResource
from dbcp.validation.tests import validate_warehouse

logger = logging.getLogger(__name__)


@dg.asset(group_name="data_warehouse", deps=["fips_tables"])
def eip_infrastructure(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """EIP Infrastructure ETL."""
    # Extract
    eip_raw_dfs = dbcp.extract.eip_infrastructure.extract()

    # Transform
    eip_transformed_dfs = dbcp.transform.eip_infrastructure.transform(eip_raw_dfs)

    return duckdb.write_tables(eip_transformed_dfs, SchemaName.DATA_WAREHOUSE)


@dg.asset(group_name="data_warehouse")
def file_modification_dates(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """Return a DF with last modified dates for all raw data inputs."""
    file_paths = load_yml_file(DATA_DIR / "file_paths.yml")
    file_df = dbcp.transform.file_modification.transform(file_paths)
    return duckdb.write_tables(file_df, SchemaName.DATA_WAREHOUSE)


@dg.asset(group_name="data_warehouse", deps=["fips_tables"])
def fyi_queue(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """Interconnection.fyi ISO Queues ETL."""
    file_paths = load_yml_file(DATA_DIR / "file_paths.yml")
    fyi_raw_dfs = dbcp.extract.fyi_queue.extract(file_paths["fyi_queue"].item())
    fyi_transformed_dfs = dbcp.transform.fyi_queue.transform(fyi_raw_dfs)
    return duckdb.write_tables(fyi_transformed_dfs, SchemaName.DATA_WAREHOUSE)


@dg.asset(group_name="data_warehouse", deps=["fips_tables"])
def columbia_local_opp(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """Columbia Local Opposition ETL."""
    # Extract
    file_paths = load_yml_file(DATA_DIR / "file_paths.yml")
    extractor = dbcp.extract.local_opposition.ColumbiaDocxParser()
    extractor.load_docx(file_paths["local_opposition"].item())
    docx_dfs = extractor.extract()

    # Transform
    transformed_dfs = dbcp.transform.local_opposition.transform(docx_dfs)

    return duckdb.write_tables(transformed_dfs, SchemaName.DATA_WAREHOUSE)


@dg.asset(group_name="data_warehouse", deps=["fips_tables"])
def pudl_tables(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """Pull tables from pudl sqlite database."""
    raw_pudl_tables = dbcp.extract.pudl_data.extract()
    return duckdb.write_tables(
        dbcp.transform.pudl_data.transform(raw_pudl_tables), SchemaName.DATA_WAREHOUSE
    )


@dg.asset(group_name="data_warehouse", deps=["fips_tables"])
def ncsl_state_permitting(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """NCSL State Permitting for Wind ETL."""
    file_paths = load_yml_file(DATA_DIR / "file_paths.yml")
    raw_df = dbcp.extract.ncsl_state_permitting.extract(
        file_paths["ncsl_state_permitting"].item()
    )

    out = dbcp.transform.ncsl_state_permitting.transform(raw_df)

    return duckdb.write_tables(out, SchemaName.DATA_WAREHOUSE)


@dg.asset(group_name="data_warehouse")
def fips_tables(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """Master state and county FIPS table ETL."""
    fips = dbcp.extract.fips_tables.extract_fips()

    file_paths = load_yml_file(DATA_DIR / "file_paths.yml")
    fips["tribal_land"] = dbcp.extract.fips_tables.extract_census_tribal_land(
        file_paths["fips_tribal_lands_uri"].item()
    )

    out = dbcp.transform.fips_tables.transform(fips)

    return duckdb.write_tables(out, SchemaName.DATA_WAREHOUSE)


@dg.asset(group_name="data_warehouse")
def justice40(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """ETL white house environmental justice dataset."""
    source_path = load_yml_file(DATA_DIR / "file_paths.yml")["justice40"].item()
    raw = dbcp.extract.justice40.extract(source_path)
    out = dbcp.transform.justice40.transform(raw)
    return duckdb.write_tables(out, SchemaName.DATA_WAREHOUSE)


@dg.asset(group_name="data_warehouse", deps=["fips_tables"])
def nrel_ordinances(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """ETL NREL state and local ordinances for wind and solar."""
    wind_source_path = load_yml_file(DATA_DIR / "file_paths.yml")[
        "nrel_wind_ordinances"
    ].item()
    solar_source_path = load_yml_file(DATA_DIR / "file_paths.yml")[
        "nrel_solar_ordinances"
    ].item()
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

    return duckdb.write_tables(nrel_transformed_dfs, SchemaName.DATA_WAREHOUSE)


@dg.asset(group_name="data_warehouse")
def protected_area_by_county(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """ETL the PAD-US intersection with TIGER county geometries."""
    source_path = load_yml_file(DATA_DIR / "file_paths.yml")[
        "padus_intersect_counties"
    ].item()
    raw_df = dbcp.extract.protected_area_by_county.extract(source_path)
    transformed = dbcp.transform.protected_area_by_county.transform(raw_df)
    return duckdb.write_tables(transformed, SchemaName.DATA_WAREHOUSE)


@dg.asset(group_name="data_warehouse")
def energy_communities_by_county(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """ETL RMI's energy communities analysis."""
    source_path = load_yml_file(DATA_DIR / "file_paths.yml")[
        "rmi_energy_communities_counties"
    ].item()
    raw_df = dbcp.extract.rmi_energy_communities.extract(source_path)
    transformed = dbcp.transform.rmi_energy_communities.transform(raw_df)
    return duckdb.write_tables(transformed, SchemaName.DATA_WAREHOUSE)


@dg.asset(group_name="data_warehouse")
def epa_avert(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """ETL EPA AVERT avoided emissions data."""
    # https://github.com/USEPA/AVERT/blob/v4.1.0/utilities/data/county-fips.txt
    path_county_region_xwalk = load_yml_file(DATA_DIR / "file_paths.yml")[
        "epa_avert_county_fips"
    ].item()
    # https://www.epa.gov/avert/avoided-emission-rates-generated-avert
    path_emission_rates = load_yml_file(DATA_DIR / "file_paths.yml")[
        "epa_avert_emission_rates"
    ].item()
    raw_dfs = dbcp.extract.epa_avert.extract(
        county_crosswalk_path=path_county_region_xwalk,
        emission_rates_path=path_emission_rates,
    )
    transformed = dbcp.transform.epa_avert.transform(raw_dfs)
    return duckdb.write_tables(transformed, SchemaName.DATA_WAREHOUSE)


@dg.asset(group_name="data_warehouse", deps=["fips_tables"])
def civis(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """ETL archived Civis county demographics and election results."""
    raw_dfs = extract_civis()
    county_fips = pd.read_sql(
        "SELECT * FROM data_warehouse.census__county_fips", duckdb.get_engine()
    )
    transformed = dbcp.transform.civis.transform(raw_dfs, county_fips=county_fips)
    return duckdb.write_tables(transformed, SchemaName.DATA_WAREHOUSE)


@dg.asset(group_name="data_warehouse")
def acp_projects(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """ETL ACP projects."""
    raw_dfs = dbcp.extract.acp_projects.extract()
    transformed = dbcp.transform.acp_projects.transform(raw_dfs)
    return duckdb.write_tables(transformed, SchemaName.DATA_WAREHOUSE)


@dg.asset(group_name="data_warehouse")
def manual_ordinances(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """ETL manually maintained ordinances."""
    raw_dfs = dbcp.extract.manual_ordinances.extract()
    transformed = dbcp.transform.manual_ordinances.transform(raw_dfs)
    return duckdb.write_tables(transformed, SchemaName.DATA_WAREHOUSE)


@dg.asset(group_name="data_warehouse", deps=["fips_tables"])
def offshore_wind(duckdb: DuckDBResource) -> dg.MaterializeResult:
    """ETL manually curated offshore wind data."""
    # get the latest version of the offshore wind data from the candidate yaml file
    projects_uri = "airtable/Offshore Wind Locations DBCP Version/Projects.json"
    locations_uri = "airtable/Offshore Wind Locations DBCP Version/Locations.json"

    es = ExtractionSettings.from_yaml(
        importlib.resources.files("dbcp").joinpath("settings.yaml")
    )
    es.update_archive_generation_numbers()

    projects_uri = es.get_full_archive_uri(projects_uri)
    locations_uri = es.get_full_archive_uri(locations_uri)

    raw_offshore_dfs = dbcp.extract.offshore_wind.extract(
        locations_uri=locations_uri, projects_uri=projects_uri
    )
    offshore_transformed_dfs = dbcp.transform.offshore_wind.transform(raw_offshore_dfs)

    return duckdb.write_tables(offshore_transformed_dfs, SchemaName.DATA_WAREHOUSE)


WAREHOUSE_DATASETS = [
    "eip_infrastructure",
    "file_modification_dates",
    "fyi_queue",
    "columbia_local_opp",
    "pudl_tables",
    "ncsl_state_permitting",
    "fips_tables",
    "justice40",
    "nrel_ordinances",
    "protected_area_by_county",
    "energy_communities_by_county",
    "epa_avert",
    "civis",
    "acp_projects",
    "manual_ordinances",
    "offshore_wind",
]


@dg.asset(group_name="data_warehouse", deps=WAREHOUSE_DATASETS)
def validated_data_warehouse(duckdb: DuckDBResource) -> None:
    """Run data warehouse validation tests."""
    validate_warehouse(engine=duckdb.get_engine())


def _data_mart_asset(module, name: str) -> dg.AssetsDefinition:
    @dg.asset(
        name=name,
        key_prefix="data_mart",
        group_name="data_mart",
        deps=[validated_data_warehouse],
    )
    def _asset(duckdb: DuckDBResource) -> dg.MaterializeResult:
        data = module.create_data_mart(engine=duckdb.get_engine())
        if isinstance(data, pd.DataFrame):
            data = {name: data}
        elif not isinstance(data, dict):
            raise TypeError(
                f"Expecting pd.DataFrame or dict of dataframes. Got {type(data)}"
            )
        return duckdb.write_tables(data, SchemaName.DATA_MART)

    return _asset


# One asset per data mart module.
data_mart_assets = [
    _data_mart_asset(module, module_info.name)
    for module, module_info in get_data_mart_modules()
]


##########################################
### DEPRECATED ETL PIPELINES ###
#############################

# def etl_lbnl_iso_queue() -> dict[str, pd.DataFrame]:
#     """LBNL ISO Queues ETL."""
#     lbnl_uri = "gs://dgm-archive/lbnl_iso_queue/queues_2024_clean_data.xlsx"
#     lbnl_raw_dfs = dbcp.extract.lbnl_iso_queue.extract(lbnl_uri)
#     lbnl_transformed_dfs = dbcp.transform.lbnl_iso_queue.transform(lbnl_raw_dfs)

#     return lbnl_transformed_dfs


# def etl_gridstatus_isoqueues():
#     """ETL gridstatus ISO queues."""
#     raw_dfs = dbcp.extract.gridstatus_isoqueues.extract()
#     transformed = dbcp.transform.gridstatus.transform(raw_dfs)
#     return transformed


# def etl_ballot_ready() -> dict[str, pd.DataFrame]:
#     """ETL Ballot Ready election data."""
#     source_uri = BR_URI
#     raw_df = dbcp.extract.ballot_ready.extract(source_uri)
#     transformed = dbcp.transform.ballot_ready.transform(raw_df)
#     return transformed
