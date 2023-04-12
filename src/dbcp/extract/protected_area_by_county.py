"""Extract data from USGS PAD-US intersected with TIGER county shapefiles."""
from pathlib import Path

import pandas as pd


def extract(path: Path) -> dict[str, pd.DataFrame]:
    """Read padus X counties dataset to pandas dataframe."""
    pad_counties = pd.read_parquet(path)  # dtypes are already set
    return {"protected_area_by_county": pad_counties}
