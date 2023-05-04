"""Extract canonical state and county FIPS tables from the addfips library."""
from importlib.resources import files
from typing import Dict

import addfips
import geopandas as gpd
import pandas as pd

import dbcp


def _extract_census_counties() -> pd.DataFrame:
    """Extract canonical county FIPS tables from census data.

    Returns:
        pd.DataFrame: output dataframes of county-level info
    """
    # ignore geometry info -- big and not currently used
    path = dbcp.extract.helpers.cache_gcs_archive_file_locally(
        "census/tl_2021_us_county.zip"
    )
    counties = gpd.read_file(path)
    return counties


def _extract_census_tribal_land() -> pd.DataFrame:
    """Extract Tribal land in the census.

    Returns:
        pd.DataFrame: output dataframes of county-level info
    """
    path = dbcp.extract.helpers.cache_gcs_archive_file_locally(
        "census/tl_2021_us_aiannh.zip"
    )
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


def extract() -> Dict[str, pd.DataFrame]:
    """Extract canonical state and county FIPS tables from census data and the addfips library.

    Returns:
        Dict[str, pd.DataFrame]: output dictionary of dataframes
    """
    fips_data = {}
    fips_data["counties"] = _extract_census_counties()
    fips_data["states"] = _extract_state_fips()
    fips_data["tribal_land"] = _extract_census_tribal_land()
    return fips_data
