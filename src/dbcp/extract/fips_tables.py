"""Extract canonical state and county FIPS tables from the addfips library."""
from importlib.resources import files
from typing import Dict

import addfips
import pandas as pd

from dbcp.constants import FIPS_CODE_VINTAGE


def extract(vintage: int = FIPS_CODE_VINTAGE) -> Dict[str, pd.DataFrame]:
    """Extract canonical state and county FIPS tables from the addfips library.

    Args:
        vintage (int, optional): which Census year to use. Defaults to FIPS_CODE_VINTAGE.

    Returns:
        Dict[str, pd.DataFrame]: output dictionary of dataframes
    """
    data_dir_path = files(addfips)
    county_csv_path = data_dir_path / addfips.addfips.COUNTY_FILES[vintage]
    state_csv_path = data_dir_path / addfips.addfips.STATES
    counties = pd.read_csv(county_csv_path, dtype=str)
    states = pd.read_csv(state_csv_path, dtype=str)
    return {"county_fips": counties, "state_fips": states}
