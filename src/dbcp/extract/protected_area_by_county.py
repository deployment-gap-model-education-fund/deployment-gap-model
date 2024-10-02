"""Extract data from USGS PAD-US intersected with TIGER county shapefiles.

This data is derived from the Protected Areas Database of the United States (PAD-US) and
intersected with TIGER county shapefiles. It was prototyped in a notebook but was never
moved into a standalone module. The data loaded here is created in
notebooks/23-tpb-check_federal_lands.ipynb. Ideally this data would be re-created in
a module and loaded here, with a disk cache if necessary for performance.
"""
from pathlib import Path

import pandas as pd


def extract(path: Path) -> dict[str, pd.DataFrame]:
    """Read padus X counties dataset to pandas dataframe."""
    pad_counties = pd.read_parquet(path)  # dtypes are already set
    return {"protected_area_by_county": pad_counties}
