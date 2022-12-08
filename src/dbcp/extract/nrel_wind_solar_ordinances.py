"""Extract NREL's dataset of wind and solar ordinances."""

from pathlib import Path
from typing import Literal

import pandas as pd


def extract(
    path: Path, wind_or_solar: Literal["solar", "wind"]
) -> dict[str, pd.DataFrame]:
    """Read NREL local ordinance databases (works for both wind and solar as of 2022).

    Args:
        path (Path): filepath

    Returns:
        Dict[str, pd.DataFrame]: output dictionary of dataframes
    """
    if wind_or_solar not in {"solar", "wind"}:
        raise ValueError(
            f"wind_or_solar must be either 'wind' or 'solar'. Given '{wind_or_solar}'"
        )

    sheets_to_read = [
        "State",
        "County, State",
        # "Value Ranges",
        # "Sheet1",
    ]
    raw_dfs = pd.read_excel(path, sheet_name=sheets_to_read)
    rename_dict = {
        "State": f"nrel_state_{wind_or_solar}_ordinances",
        "County, State": f"nrel_local_{wind_or_solar}_ordinances",
    }
    raw_dfs = {rename_dict[key]: df for key, df in raw_dfs.items()}

    return raw_dfs
