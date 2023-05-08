"""Transform and normalize offshore wind location and project data from Airtable."""

import pandas as pd

from dbcp.transform.helpers import add_county_fips_with_backup_geocoding
from pudl.helpers import simplify_columns


def _transform_columns(
    raw_df: pd.DataFrame, *, cols_to_drop: list[str], rename_dict: dict[str, str]
) -> None:
    simplify_columns(raw_df)
    raw_df.drop(columns=cols_to_drop, inplace=True)
    raw_df.rename(columns=rename_dict, inplace=True)
    return


def _association_table_from_csv_array(ser: pd.Series, name="id") -> pd.Series:
    """Convert a series of CSV string arrays to a long-format association table.

    Example input:
    pd.Series(['1,2', '1,3,7'], index=['a', 'b'])

    Example Output (note: dtype is still string):
    'a'  '1'
    'a'  '2'
    'b'  '1'
    'b'  '3'
    'b'  '7'
    """
    expanded = ser.str.split(",", expand=True)
    out = (
        expanded.melt(value_vars=expanded.columns, value_name=name, ignore_index=False)
        .dropna()
        .loc[:, name]
        .str.strip()
    )
    return out


def _id_association_table_from_csv_array(
    ser: pd.Series, name="project_id"
) -> pd.DataFrame:
    """Special case of _association_table_from_csv_array for the integer ID arrays in this particular dataset."""
    out = (
        _association_table_from_csv_array(ser, name=name)
        .astype(int)
        .reset_index()
        .sort_values(["location_id", "project_id"])
    )
    return out


def _add_geocoded_locations(transformed_locs: pd.DataFrame) -> None:
    """Standardize place names and fetch county FIPS codes.

    We need to use a two-pass geocoding system because both city and county names are present in the "city" column.
    The first pass operates on a concatenated "city, county" field, then any NaN results are filled using just "city".

    This is one way to fix the following problem:
    Google Maps Platform resolves city/county ambiguity by returning the higher level of government.
    That messes up some of the `geocoded_locality_type` results (Albany, NY is in Albany County, NY). It also
    occasionally messes up the FIPS when the identically-named city and county are not colocated
    (Houston, TX is not in Houston County, TX).

    Args:
        transformed_locs (pd.DataFrame): locations df after other cleaning has been performed
    """
    # this code is ugly and procedural for the sake of making inplace operations.
    # And all to save copy operations on a dataframe that is... 50 rows lol. Dumb!
    transformed_locs["city_county"] = (
        transformed_locs["raw_city"] + ", " + transformed_locs["raw_county"]
    )
    first_pass = add_county_fips_with_backup_geocoding(
        transformed_locs[["city_county", "raw_state_abbrev"]],
        state_col="raw_state_abbrev",
        locality_col="city_county",
    )
    transformed_locs.drop(columns=["city_county"], inplace=True)

    nan_fips_idx = first_pass["county_id_fips"].isna()
    second_pass = add_county_fips_with_backup_geocoding(
        transformed_locs.loc[nan_fips_idx, ["raw_city", "raw_state_abbrev"]],
        state_col="raw_state_abbrev",
        locality_col="raw_city",
    )

    cols_to_fill = [
        "county_id_fips",
        "geocoded_locality_name",
        "geocoded_locality_type",
        "geocoded_containing_county",
    ]
    first_pass.update(second_pass.loc[:, cols_to_fill])
    transformed_locs.loc[:, cols_to_fill] = first_pass[cols_to_fill]
    return


def _validate_raw_data(raw_dfs: dict[str, pd.DataFrame]) -> None:
    proj = raw_dfs["offshore_projects"].copy()
    locs = raw_dfs["offshore_locations"].copy()
    for df in (proj, locs):
        assert (
            not df.eq("").any().any()
        ), f"Empty strings in df with index {df.index.name}"
        assert df.index.is_unique, f"Index {df.index.name} is not unique."

    assert (
        proj["Size (megawatts)"].lt(100).sum() == 1
    ), "Found unusually small project. Block Island is the only known exception."
    assert proj["Size (megawatts)"].max() < 10_000, "Found unusually large project."

    assert (
        proj["Recipient State"].str.contains("TBD").sum() == 1
    ), "More missing recipient states than expected."

    assert (
        proj["Online date"].lt(2022).sum() == 1
    ), "Found project with erroneously early online_date. Block Island is the only known exception."
    assert (
        proj["Online date"].max() < 2040
    ), "Found project with erroneously late online_date."

    # check referential symmetry (should be handled by Airtable, but check anyway)
    locations_to_landings = pd.MultiIndex.from_frame(
        _id_association_table_from_csv_array(locs["Cable project IDs"])
    )
    landing_to_locations = pd.MultiIndex.from_frame(
        _id_association_table_from_csv_array(
            proj["Cable Location IDs"], name="location_id"
        )[
            ["location_id", "project_id"]
        ]  # reorder columns
    )
    assert locations_to_landings.symmetric_difference(landing_to_locations).empty

    locations_to_ports = pd.MultiIndex.from_frame(
        _id_association_table_from_csv_array(locs["assembly/manufac project IDs"])
    )
    ports_to_locations = pd.MultiIndex.from_frame(
        _id_association_table_from_csv_array(
            proj["Port Location IDs"], name="location_id"
        )[["location_id", "project_id"]]
    )
    assert locations_to_ports.symmetric_difference(ports_to_locations).empty
    return


def transform(raw_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Transform offshore wind data."""
    _validate_raw_data(raw_dfs=raw_dfs)
    proj = raw_dfs["offshore_projects"].copy()
    locs = raw_dfs["offshore_locations"].copy()
    # NOTE: the following column names are not raw names!
    # They are the results of simplify_columns(), which is called in _transform_columns
    proj_cols_to_drop = [
        "proposed_cable_landing",
        "county_of_cable_landing",
        "port_locations",
        "cop_in_process",
    ]
    proj_rename_dict = {
        "size_megawatts": "capacity_mw",
        "online_date": "proposed_completion_year",
    }
    locs_cols_to_drop = [
        "cable_landing_s",
        "assembly_manufacturing",
    ]
    locs_rename_dict = {
        "city": "raw_city",
        "state": "raw_state_abbrev",
        "county": "raw_county",
        "county_fips": "raw_county_fips",
    }
    _transform_columns(
        proj, cols_to_drop=proj_cols_to_drop, rename_dict=proj_rename_dict
    )
    _transform_columns(
        locs, cols_to_drop=locs_cols_to_drop, rename_dict=locs_rename_dict
    )

    proj.loc[:, "recipient_state"].replace({"TBD": pd.NA}, inplace=True)

    transformed_dfs = {}
    transformed_dfs[
        "offshore_wind_cable_landing_association"
    ] = _id_association_table_from_csv_array(locs["cable_project_ids"])
    transformed_dfs[
        "offshore_wind_port_association"
    ] = _id_association_table_from_csv_array(locs["assembly_manufac_project_ids"])
    # CSV array fields no longer needed (proj CSV fields could be dropped after _validate_raw_data())
    locs.drop(
        columns=["cable_project_ids", "assembly_manufac_project_ids"], inplace=True
    )
    proj.drop(columns=["cable_location_ids", "port_location_ids"], inplace=True)

    _add_geocoded_locations(locs)
    # move index into columns
    locs.reset_index(inplace=True)
    proj.reset_index(inplace=True)
    transformed_dfs["offshore_wind_projects"] = proj
    transformed_dfs["offshore_wind_locations"] = locs

    return transformed_dfs
