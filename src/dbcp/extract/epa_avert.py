"""Retrieve data from EPA AVERT avoided carbon modeling."""
from pathlib import Path

import pandas as pd


def extract(
    county_crosswalk_path: Path, emission_rates_path: Path
) -> dict[str, pd.DataFrame]:
    """Read EPA AVERT emissions database.

    Args:
        county_crosswalk_path (Path): path to county crosswalk file
        emission_rates_path (Path): path to AVERT emission rates file

    Returns:
        Dict[str, pd.DataFrame]: output dictionary of dataframes
    """
    emission_rates = _read_emissions_and_capacity_factors(emission_rates_path)
    crosswalk = _read_crosswalk(county_crosswalk_path)
    emission_rates.update(dict(avert_county_region_assoc=crosswalk))
    return emission_rates


def _read_crosswalk(path: Path) -> pd.DataFrame:
    """Read EPA AVERT county crosswalk."""
    rename_dict = {
        "State and County FIPS Code": "county_id_fips",
        "AVERT Region": "avert_region",
    }
    df = pd.read_csv(
        path,
        sep="\t",
        header=0,
        usecols=["State and County FIPS Code", "AVERT Region"],
        dtype="string",
    ).rename(columns=rename_dict, copy=False)
    return df


def _read_emissions_and_capacity_factors(path: Path) -> dict[str, pd.DataFrame]:
    """Read EPA AVERT emission rates from excel file."""
    cap_factors = (
        pd.read_excel(path, sheet_name="Capacity factors", skiprows=1, skipfooter=1)
        .rename(columns={"Unnamed: 0": "avert_region"}, copy=False)
        .rename(columns=lambda x: x.lower().replace(" ", "_"), copy=False)
    )
    assert cap_factors.shape == (14, 5)
    # emissions sheet has 7 tables in a grid. We only want the CO2 table.
    # Cutting leading and trailing rows removes 5 of them.
    # iloc[:, :7] removes the adjacent table.
    emissions = (
        pd.read_excel(path, sheet_name="2022", skiprows=16, skipfooter=44)
        .iloc[:, :7]
        .rename(columns=lambda x: x.lower().replace(" ", "_"), copy=True)
        .rename(columns={"\xa0": "avert_region"}, copy=False)
    )
    assert emissions.iat[-1, 0] == "Texas"
    return {"avert_capacity_factors": cap_factors, "avert_emissions_factors": emissions}


if __name__ == "__main__":
    # debugging entry point
    path_county_region_xwalk = (
        Path(__file__).parents[2] / "data/raw/avert_county-fips.txt"
    )
    assert path_county_region_xwalk.exists()
    path_emission_rates = (
        Path(__file__).parents[2] / "data/raw/avert_emission_rates_04-25-23.xlsx"
    )
    assert path_emission_rates.exists()
    crosswalk = _read_crosswalk(path_county_region_xwalk)
    emission_rates = _read_emissions_and_capacity_factors(path_emission_rates)
