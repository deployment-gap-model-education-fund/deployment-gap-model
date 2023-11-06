"""Transform EPA AVERT avoided emissions database."""
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _capacity_factor_transform(cap_factors: pd.DataFrame) -> pd.DataFrame:
    """Transform capacity factors table."""
    cap_factors = (
        cap_factors.replace("-", np.nan)
        .melt(
            id_vars=["avert_region"],
            var_name="resource_type",
            value_name="capacity_factor",
        )
        .astype(
            {
                "avert_region": "string",
                "resource_type": "string",
                "capacity_factor": np.float32,
            },
            copy=False,
        )
    )

    return cap_factors


def _emissions_transform(emissions: pd.DataFrame) -> pd.DataFrame:
    """Transform emissions table."""
    lbs_to_tonnes = 1 / 2204.62
    emissions = (
        emissions.replace("-", np.nan)
        # remove energy efficiency columns
        .drop(columns=[col for col in emissions.columns if col.endswith("_ee")])
        .melt(
            id_vars=["avert_region"],
            var_name="resource_type",
            value_name="tonnes_co2_per_mwh",
        )
        .astype(
            {
                "avert_region": "string",
                "resource_type": "string",
                "tonnes_co2_per_mwh": np.float32,
            },
            copy=False,
        )
    )
    emissions["tonnes_co2_per_mwh"] *= lbs_to_tonnes
    return emissions


def _crosswalk_transform(crosswalk: pd.DataFrame) -> pd.DataFrame:
    # nothing for now
    return crosswalk


def transform(raw_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Transform EPA AVERT emissions database.

    Args:
        raw_dfs (dict[str, pd.DataFrame]): input dictionary of dataframes

    Returns:
        Dict[str, pd.DataFrame]: output dictionary of dataframes
    """
    cap_factors = _capacity_factor_transform(raw_dfs["avert_capacity_factors"])
    emissions = _emissions_transform(raw_dfs["avert_emissions_factors"])
    avert_factors = cap_factors.merge(
        emissions, on=["avert_region", "resource_type"], how="outer"
    )
    assert (
        avert_factors[["avert_region", "resource_type"]]
        .isna()
        .sum()
        .eq(0)
        .all()
        .squeeze()
    )
    avert_regions = set(avert_factors["avert_region"].unique())
    assert "Alaska" not in avert_regions
    assert "Hawaii" not in avert_regions
    avert_factors["co2e_tonnes_per_year_per_mw"] = (
        avert_factors["tonnes_co2_per_mwh"] * avert_factors["capacity_factor"] * 8766
    )

    crosswalk = _crosswalk_transform(raw_dfs["avert_county_region_assoc"])
    assert set(crosswalk["avert_region"].unique()).symmetric_difference(
        avert_regions
    ) == set(["Alaska", "Hawaii"])
    return {
        "avert_avoided_emissions_factors": avert_factors,
        "avert_county_region_assoc": crosswalk,
    }


if __name__ == "__main__":
    # debugging entry point
    from pathlib import Path

    from dbcp.extract.epa_avert import extract

    path_county_region_xwalk = (
        Path(__file__).parents[2] / "data/raw/avert_county-fips.txt"
    )
    assert path_county_region_xwalk.exists()
    path_emission_rates = (
        Path(__file__).parents[2] / "data/raw/avert_emission_rates_04-25-23.xlsx"
    )
    assert path_emission_rates.exists()
    raw_dfs = extract(
        county_crosswalk_path=path_county_region_xwalk,
        emission_rates_path=path_emission_rates,
    )
    transformed_dfs = transform(raw_dfs)
