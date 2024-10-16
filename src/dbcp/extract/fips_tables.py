"""Extract canonical state and county FIPS tables from the addfips library."""
from functools import lru_cache
from importlib.resources import files
from typing import Dict

import addfips
import geopandas as gpd
import pandas as pd

import dbcp

# originally from https://www2.census.gov/geo/tiger/TIGER2021/
CENSUS_URI = "gs://dgm-archive/census/tl_2021_us_county.zip"
TRIBAL_LANDS_URI = "gs://dgm-archive/census/tl_2021_us_aiannh.zip"


@lru_cache(maxsize=1)  # county boundaries are also used in some transform modules
def _extract_census_counties(census_uri: str) -> pd.DataFrame:
    """Extract canonical county FIPS tables from census data.

    Args:
        census_uri: path to zipped shapefiles.
    """
    path = dbcp.extract.helpers.cache_gcs_archive_file_locally(census_uri)
    counties = gpd.read_file(path)
    return counties


def extract_census_tribal_land(archive_uri: str) -> pd.DataFrame:
    """Extract Tribal land in the census.

    Args:
        archive_uri: path of file to extract from the dgm-archive GCS bucket.

    Returns:
        output dataframes of county-level info.
    """
    path = dbcp.extract.helpers.cache_gcs_archive_file_locally(archive_uri)
    counties = gpd.read_file(path)
    return counties


def _extract_state_fips() -> pd.DataFrame:
    """Extract canonical state and county FIPS tables from census data and the addfips library.

    Args:
        vintage (int, optional): which Census year to use. Defaults to FIPS_CODE_VINTAGE.

    Returns:
        Dict[str, pd.DataFrame]: output dictionary of dataframes
    """
    data_dir_path = files(addfips)
    state_csv_path = data_dir_path / addfips.addfips.STATES
    states = pd.read_csv(state_csv_path, dtype=str)
    return states


def extract_fips(census_uri: str) -> Dict[str, pd.DataFrame]:
    """Extract canonical state and county FIPS tables from census data and the addfips library.

    Returns:
        Dict[str, pd.DataFrame]: output dictionary of dataframes
    """
    fips_data = {}
    fips_data["counties"] = _extract_census_counties(census_uri=census_uri)
    fips_data["states"] = _extract_state_fips()
    return fips_data
