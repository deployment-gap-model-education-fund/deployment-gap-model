"""Transformations for the PAD-US intersection with TIGER county geometries."""
import pandas as pd


def transform(raw_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Transform the PAD-US intersection with TIGER county geometries."""
    cols_to_drop = [  # already in county_fips table
        "county_land_area_sq_meters",
        "county_water_area_sq_meters",
        "internal_point_latitude",
        "internal_point_longitude",
    ]
    transformed = raw_dfs["protected_area_by_county"].drop(columns=cols_to_drop)  # copy
    transformed_dfs = {"protected_area_by_county": transformed}
    return transformed_dfs
