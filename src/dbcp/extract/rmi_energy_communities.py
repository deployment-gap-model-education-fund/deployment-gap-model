"""Extract data from RMI/Catalyst energy communities analysis.

Source repo: https://github.com/catalyst-cooperative/rmi-energy-communities
"""
from pathlib import Path

import pandas as pd


def extract(path: Path) -> dict[str, pd.DataFrame]:
    """Read county-level qualification dataset to pandas dataframe."""
    rmi_ec = pd.read_parquet(path)
    return {"energy_communities_by_county": rmi_ec}
