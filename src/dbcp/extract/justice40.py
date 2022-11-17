"""Extract data from Justice 40 CSV for analysis."""
from pathlib import Path

import pandas as pd


def extract(path: Path) -> dict[str, pd.DataFrame]:
    """Read raw Justice40 dataset to pandas dataframe."""
    # source: https://screeningtool.geoplatform.gov/en/downloads
    j40 = pd.read_csv(
        path,
        compression="zip",
        delimiter=",",
    )
    return {"justice40": j40}
