"""Transformations for RMI's energy communities analysis."""
import pandas as pd

from dbcp.transform.helpers import add_county_fips_with_backup_geocoding


def transform(raw_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Transform RMI's energy communities analysis."""
    transformed = raw_dfs["energy_communities_by_county"].reset_index(
        drop=False
    )  # copy
    rename_dict = {
        "county_id_fips": "raw_county_id_fips",
        "state_name": "raw_state_name",
        "county_name": "raw_county_name",
        "num_brownfields": "n_brownfields",
        "num_coal_qualifying_tracts": "n_coal_qualifying_tracts",
        "percent_of_county_coal_qualified": "coal_qualifying_area_fraction",
    }
    transformed.rename(columns=rename_dict, inplace=True)
    # fix two counties whose FIPS changed from 2010 to 2015
    transformed = add_county_fips_with_backup_geocoding(
        transformed, state_col="raw_state_name", locality_col="raw_county_name"
    )

    # fix one null FIPS (Villalba Municipio, Puerto Rico)
    fips_is_nan = transformed["county_id_fips"].isna()
    assert (
        fips_is_nan.sum() == 1
    ), f"Assumption violation: expected 1 null FIPS, got {fips_is_nan.sum()}"
    transformed.loc[:, "county_id_fips"] = transformed.loc[:, "county_id_fips"].fillna(
        transformed.loc[:, "raw_county_id_fips"]
    )

    cols_to_drop = [
        "state_id_fips",
        "geocoded_locality_type",  # all counties
        "geocoded_containing_county",  # all counties
    ]
    transformed.drop(columns=cols_to_drop, inplace=True)
    transformed_dfs = {"energy_communities_by_county": transformed}
    return transformed_dfs
