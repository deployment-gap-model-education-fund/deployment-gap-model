"""Extract data from RMI's energy communities analysis."""
from pathlib import Path

import pandas as pd


def extract(path: Path) -> dict[str, pd.DataFrame]:
    """Read county-level qualification dataset to pandas dataframe."""
    rmi_ec = pd.read_parquet(path)
    return {"energy_communities_by_county": rmi_ec}
