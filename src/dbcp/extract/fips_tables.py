"""Extract canonical state and county FIPS tables from the addfips library."""
from importlib.resources import files
from pathlib import Path
from typing import Dict

import addfips
import geopandas as gpd
import pandas as pd


def _extract_census(census_path: Path) -> pd.DataFrame:
    """Extract canonical county FIPS tables from census data.

    Args:
        census_path (Path): path to zipped shapefiles.

    Returns:
        pd.DataFrame: output dataframes of county-level info
    """
    # ignore geometry info -- big and not currently used
    counties = gpd.read_file(census_path, ignore_geometry=True)
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


def extract(census_path: Path) -> Dict[str, pd.DataFrame]:
    """Extract canonical state and county FIPS tables from census data and the addfips library.

    Args:
        vintage (int, optional): which Census year to use. Defaults to FIPS_CODE_VINTAGE.

    Returns:
        Dict[str, pd.DataFrame]: output dictionary of dataframes
    """
    counties = _extract_census(census_path=census_path)
    states = _extract_state_fips()
    return {"county_fips": counties, "state_fips": states}
