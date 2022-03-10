from typing import Dict
from importlib.resources import files

import pandas as pd
import addfips

from dbcp.constants import FIPS_CODE_VINTAGE

def extract(vintage: int=FIPS_CODE_VINTAGE) -> Dict[str, pd.DataFrame]:
    data_dir_path = files(addfips)
    county_csv_path = data_dir_path / addfips.addfips.COUNTY_FILES[vintage]
    state_csv_path = data_dir_path / addfips.addfips.STATES
    counties = pd.read_csv(county_csv_path, dtype=str)
    states = pd.read_csv(state_csv_path, dtype=str)
    return {'county_fips': counties,
            'state_fips': states}
