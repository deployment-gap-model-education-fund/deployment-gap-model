"""Transform and clean ACP projects data."""

import hashlib
import re
from collections.abc import Callable
from functools import partial

import geopandas as gpd
import numpy as np
import pandas as pd

from dbcp.extract.fips_tables import CENSUS_URI, _extract_census_counties
from dbcp.transform.helpers import add_county_fips_with_backup_geocoding


def _rename_columns(raw_cols: pd.Index) -> pd.Index:
    """Rename columns to snake case."""

    def _camel_to_snake(name: str) -> str:
        """Roughly convert camel case to snake case. Not perfect."""
        name = re.sub("([^A-Z_])([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()

    renamed = [_camel_to_snake(col) for col in raw_cols]
    # fix a few cases with adjacent capital letters
    rename_dict = {
        "pparates": "ppa_rates",
        "ppastart_years": "ppa_start_years",
        "ppaend_years": "ppa_end_years",
        "ppadurations": "ppa_durations",
        "isortos": "iso_rtos",
        "eiaplant_code": "eia_plant_code",
    }
    out = ["raw_" + rename_dict.get(col, col) for col in renamed]
    return pd.Index(out)


def _col_transform_status(ser: pd.Series) -> pd.Series:
    """Transform status column."""
    out = ser.str.strip().replace("", pd.NA)

    # set membership validation
    expected_values = {
        "Online",
        "Under Construction",
        "Advanced Development",
        "Decommissioned",
        "Online | Decommissioned",
        pd.NA,
    }
    is_expected = out.isin(expected_values)
    assert is_expected.all(), (
        f"Unexpected status values: {out[~is_expected].value_counts()}"
    )
    return out


def _col_transform_eia_plant_code(ser: pd.Series) -> pd.Series:
    """Transform eia_plant_code column."""
    # Has missing values so gets interpreted as float. Cast to Int32.
    # But first check for non-integer IDs just in case.
    is_int = ser.mod(1).fillna(0).eq(0)
    assert is_int.all(), f"Non-integer EIA IDs: {ser[~is_int].value_counts()}"
    out = ser.astype(pd.Int32Dtype())
    return out


def _col_transform_phase_type(ser: pd.Series) -> pd.Series:
    """Transform phase_type column."""
    out = ser.str.strip().replace("", pd.NA)

    # set membership validation
    expected_values = {
        "Solar",
        "Wind",
        "Storage",
        "Offshore Wind",
        pd.NA,
    }
    is_expected = out.isin(expected_values)
    assert is_expected.all(), (
        f"Unexpected status values: {out[~is_expected].value_counts()}"
    )
    return out


def _col_transform_iso_rtos(ser: pd.Series) -> pd.Series:
    # Simplify multi-valued items by taking the first array item.
    # I think this is a reasonable simplification because only 0.37% are multivalued.
    # But first check that multi-valued items are still a small minority of records
    is_multi = ser.str.contains("|", regex=False).fillna(False)
    assert is_multi.mean() < 0.01, (
        f"Too many multi-valued ISO/RTOS: {ser[is_multi].value_counts()}"
    )
    out = ser.str.split("|", regex=False).str[0].str.strip().astype(pd.StringDtype())

    # Standardize some variations
    mapping = {
        "ISO New England": "ISONE",  # make consistent with LBNL/Gridstatus
        "ISO-NE": "ISONE",  # make consistent with LBNL/Gridstatus
        "Midcontinent Independent System Operator (MISO)": "MISO",
        "PJM Interconnection": "PJM",
        "Southwest Power Pool": "SPP",
    }
    out = out.replace(mapping)

    # set membership validation
    expected_values = {
        "CAISO",
        "ERCOT",
        "ISONE",
        "MISO",
        "NYISO",
        "PJM",
        "SPP",
        "WECC-RMRG",
        pd.NA,
    }
    is_expected = out.isin(expected_values)
    assert is_expected.all(), (
        f"Unexpected ISO/RTOS values: {out[~is_expected].value_counts()}"
    )
    return out


def _col_transform_owner_types(ser: pd.Series, full_df: pd.DataFrame) -> pd.Series:
    # Simplify multi-valued items by taking the first array item.
    # Only 0.15% of proposed projects (1.4% overall) are multivalued.
    # But first check that multi-valued items are still a small minority of records
    is_multi = ser.str.contains("|", regex=False).fillna(False)
    assert is_multi.mean() < 0.02, (
        f"Too many multi-valued owner types: {ser[is_multi].value_counts()}"
    )
    out = (
        ser.str.split("|", regex=False, n=1)
        .str[0]
        .str.strip()
        .astype(pd.StringDtype())
        .replace("Unknown", pd.NA)
    )
    # Map "Investor Owned" values to "IPP" when the owner is likely
    # not a utility company. In the future, "Investor Owned" could refer
    # to "Utility: IOU" instead of "IPP"
    n_max_investor_owned_owner_type_per_date = (
        full_df[full_df["raw_owner_types"].str.contains("Investor Owned")]
        .groupby("report_date")
        .size()
        .max()
    )
    n_known_investor_owned = 1
    assert n_max_investor_owned_owner_type_per_date == n_known_investor_owned, f"""
        Found {n_max_investor_owned_owner_type_per_date} 'Investor Owned' owner types from one report date, expected {n_known_investor_owned}:
        {full_df[full_df["raw_owner_types"].str.contains("Investor Owned")][["raw_owner_types", "raw_owners", "report_date"]]}
        If these are all IPPs, then increase the number of expected.
        If there is a non-IPP, then replace owner type value with correct value, i.e. 'Utility: IOU'.
        """
    out.loc[out == "Investor Owned"] = "IPP"
    # set membership validation
    expected_values = {
        "C&I",
        "Community",
        "Government: Federal",
        "Government: Local",
        "Government: Military",
        "Government: State",
        "IPP",
        "IPP: YieldCo",
        "Individual",
        "School",
        "Tribal",
        "Utility: Cooperative",
        "Utility: Federal Power Authority",
        "Utility: IOU",
        "Utility: Municipal",
        "Utility: Political Subdivison",
        "Utility: State",
        pd.NA,
    }
    is_expected = out.isin(expected_values)
    assert is_expected.all(), (
        f"Unexpected owner type values: {out[~is_expected].value_counts()}"
    )
    return out


def _col_transform_mw_total_capacity(
    ser: pd.Series, full_df: pd.DataFrame
) -> pd.Series:
    """Redefine total capacity as existing capacity or total decommissioned capacity."""
    # This transform requires access to the full DataFrame to pull in the Status and
    # other capacity columns.
    assert ser.min() >= 0, "Found a negative total capacity"
    assert ser.max() < 4000, "Found an unreasonably large total capacity >= 4 GW"
    # In the raw data, Total Capacity = sum(other capacities EXCEPT Decommissioned).
    # For our purposes it makes more sense to put them all in the same column, even
    # though we likely won't use decommissioned capacity.
    # Note: this does not combine capacity at plants with both online and decommissioned
    # phases.

    # Confirm that decommisioned capacity is always zero
    is_decom = full_df["raw_status"] == "Decommissioned"
    assert ser.loc[is_decom].eq(0).all(), "Found non-zero Decommissioned capacity."
    out = ser.copy()
    out.loc[is_decom] = full_df.loc[is_decom, "raw_mw_decommissioned_capacity"]
    return out


def _transform_location_cols(
    full_df: pd.DataFrame, county_shapes: gpd.GeoDataFrame
) -> pd.DataFrame:
    """Clean state, county, and lat/lon columns simultaneously."""
    # County FIPS codes can be defined by either state and county names or by lat/lon.
    # Where lat/lon and state/county conflict, I'll use the state/county values and set
    # the lat/lon to null.
    # Also, some state/county entries are multi-valued. I'll simplify those to a single
    # value using the lat/lon point, if available. Otherwise, I'll default to the first
    # value.
    # The reason I don't normalize multivalued entries into a m:m table is that
    # 1) multivalued entries are rare (<0.1% of proposed phases, 2.7% of all phases)
    # 2) the ultimate use of this table is to merge it with 860m data, which does not
    #    have multivalued entries.

    # first check dtypes
    assert full_df["raw_avg_latitude"].dtype == pd.Float64Dtype(), (
        "Latitude is not float64"
    )
    assert full_df["raw_avg_longitude"].dtype == pd.Float64Dtype(), (
        "Longitude is not float64"
    )
    county_shapes["GEOID"] = county_shapes["GEOID"].astype(pd.StringDtype())
    points = gpd.GeoSeries.from_xy(
        full_df["raw_avg_longitude"].astype(np.float64),
        full_df["raw_avg_latitude"].astype(np.float64),
        index=full_df.index,
        crs=county_shapes.crs,
    ).to_frame("lat_lon")

    fips_lat_lon = points.sjoin(
        county_shapes.loc[
            :,
            [
                "GEOID",  # FIPS code
                "geometry",  # polygon
                "NAMELSAD",  # county name
            ],
        ],
        how="left",
        predicate="within",
    ).drop(columns="index_right")

    is_multivalued = full_df["raw_states"].str.contains("|", regex=False).fillna(
        False
    ) | full_df["raw_counties"].str.contains("|", regex=False).fillna(False)
    fips_state_county = (
        add_county_fips_with_backup_geocoding(
            full_df.loc[~is_multivalued, ["raw_states", "raw_counties"]].apply(
                lambda s: s.str.strip(), axis=1
            ),
            state_col="raw_states",
            locality_col="raw_counties",
        )
        .rename(
            columns={
                "county_id_fips": "geocoded_county_id_fips",
                "state_id_fips": "geocoded_state_id_fips",
            }
        )
        .convert_dtypes()
    )
    joined = fips_lat_lon.join(fips_state_county, how="outer").join(
        full_df[["raw_avg_longitude", "raw_avg_latitude"]].rename(
            columns=lambda x: x[4:]  # remove 'raw_'. These will be output column names
        ),
        how="left",
    )
    # Important to differentiate between missing values in the original data (lat/lon or
    # state/county) and missing FIPS due to failed spatial joins or geocoding.
    # Split the results into a few cases:
    # 1) lat/lon and state/county are both missing or both derived FIPS agree (no action
    #    needed. 90% of cases)
    # 2) one of the sources is missing (no fips) but the other is present and
    #    successfully produces a FIPS. (simply fillna. 9% of cases)
    # 3) one of various conflicts (<1% of cases):
    #    a) both exist but disagree (arbitarily use state/county FIPS and set lat/lon to
    #       null. This case is about 1/3 of remaining cases)
    #    b) lat/lon present but spatial join fails and state is present but county is
    #       missing. These are all offshore projects with lat/lon in the ocean. Note
    #       that sometimes geocoding returns a value because it interprets "<blank>, NY"
    #       as New York City. (Keep lat/lon, set FIPS to null. ~1/3 of remaining cases.)
    #    c) The remaining third of remaining cases are spread over a few different long
    #       tail scenarios that I won't bother making individual fixes for. Most are
    #       functionally resolved (though perhaps incorrectly) with .fillna().

    has_null_raw_county = full_df["raw_counties"].str.strip().replace("", pd.NA).isna()
    # anywhere raw_county is null, geocoded county FIPS should be null but state fips
    # should be included (some erroneous county results for offshore projects where
    # "<blank>, NY" is mapped to New York City)
    joined.loc[
        has_null_raw_county,
        [
            "geocoded_county_id_fips",
            "geocoded_locality_name",
            "geocoded_locality_type",
            "geocoded_containing_county",
        ],
    ] = pd.NA
    joined["county_id_fips"] = joined["geocoded_county_id_fips"].fillna(
        joined["GEOID"].astype(pd.StringDtype())
    )
    # if lat/lon is present but fails to join to any county, set lat/lon to null UNLESS
    # the project is an offshore wind project. Those are in the ocean, not in a county.
    is_offshore = full_df["raw_phase_type"].str.strip() == "Offshore Wind"
    has_point = ~joined["lat_lon"].is_empty
    failed_sjoin = joined["GEOID"].isna()
    joined.loc[
        failed_sjoin & has_point & ~is_offshore, ["avg_latitude", "avg_longitude"]
    ] = pd.NA
    joined.rename(columns={"GEOID": "census_county_id_fips"}, inplace=True)

    # fill in any remaining missing and multi-valued entries with the first value
    is_missing_multi = joined["county_id_fips"].isna() & is_multivalued
    first_vals = full_df.loc[is_missing_multi, ["raw_states", "raw_counties"]].apply(
        lambda s: s.str.split("|", n=1).str[0].str.strip(), axis=1
    )
    first_geocoded = (
        add_county_fips_with_backup_geocoding(
            first_vals, state_col="raw_states", locality_col="raw_counties"
        )
        .rename(
            columns={
                "county_id_fips": "geocoded_county_id_fips",
                "state_id_fips": "geocoded_state_id_fips",
            }
        )
        .convert_dtypes()
    )
    geo_cols = [
        "geocoded_locality_name",
        "geocoded_locality_type",
        "geocoded_containing_county",
        "geocoded_county_id_fips",
        "geocoded_state_id_fips",
    ]
    # fill in new vals
    joined.loc[:, geo_cols] = joined.loc[:, geo_cols].fillna(first_geocoded[geo_cols])
    joined["county_id_fips"] = joined["county_id_fips"].fillna(
        joined["geocoded_county_id_fips"]
    )
    # to define state FIPS, use the corresponding county FIPS where it exists. Sometimes
    # (about 60 cases) only the raw_state is present (no county and either no lat/lon or
    # lat/lon is in the ocean), so I'll use that too.
    joined["state_id_fips"] = (
        joined["county_id_fips"].str[:2].fillna(joined["geocoded_state_id_fips"])
    )
    out_cols = [
        "state_id_fips",
        "county_id_fips",
        "avg_latitude",
        "avg_longitude",
        "geocoded_locality_name",
        "geocoded_locality_type",
        "geocoded_containing_county",
        "geocoded_county_id_fips",
        "geocoded_state_id_fips",
        "census_county_id_fips",
    ]
    out = joined[out_cols]
    return out


def _make_surrogate_key(raw_df: pd.DataFrame) -> pd.Series:
    """Create a surrogate key from several data columns.

    The surrogate key is a hash of several columns that should uniquely
    identify a row. Light string cleaning is performed on the raw data
    to avoid minor changes breaking backwards compatibility. While
    (ProjectName, PhaseName) is unique, it may not be a reliable key in
    the future. Instead,
    (ProjectName, PhaseName, PhaseType, MW_Total_Capacity, States, Counties)
    is used as the unique key.

    Arguments:
        raw_df (pd.DataFrame): The raw dataframe to create a key for.

    """
    # The surrogate key is a hash of several columns that should uniquely identify a row
    # The main design decisions are whether to use raw data or transformed data as
    # inputs, and how many columns to include in the hash.
    # The transformed data risks changes in the transform functions that break backwards
    # compatibility. The raw data risks changes in the raw data that break backwards
    # compatibility. I'll use the raw data but use mild string cleaning to avoid minor
    # changes.

    # (ProjectName, PhaseName) is unique but I'm not sure if we can rely on it in the
    # future; there are collisions in each column individually but ACP might not allow
    # collisions on the tuples. I'm just not sure.
    # (ProjectName, PhaseName, PhaseType, MW_Total_Capacity, States, Counties, Owners)
    # pretty much fully specifies a project. But this maximally unique key includes many
    # columns, which risks breaking linkages over time if any values change, such as a
    # change in capacity or ownership.
    # I'll drop Owners because it seems very likely to change over time.
    pk = [
        "ProjectName",
        "PhaseName",
        "PhaseType",
        "MW_Total_Capacity",
        "States",
        "Counties",
    ]
    dupes = raw_df.duplicated(subset=pk, keep=False)
    assert not dupes.any(), f"Uniqueness violation: {dupes.sum()} duplicate PKs found."
    assert raw_df["MW_Total_Capacity"].dtype == pd.Float64Dtype(), (
        "Capacity is not float dtype"
    )

    to_hash = raw_df.loc[:, pk].copy()
    str_cols = to_hash.select_dtypes(include="string").columns
    to_hash[str_cols] = to_hash[str_cols].apply(
        lambda s: s.str.strip().str.lower(), axis=1
    )
    # Protect against floating point representation shenanigans by printing capacity to
    # 3 decimal places (the max observed precision in the data as of 2024-07-28)
    to_hash["MW_Total_Capacity"] = to_hash["MW_Total_Capacity"].apply(
        lambda num: f"{num:.3f}"
    )
    concat = to_hash.fillna("").apply(" ".join, axis=1)
    out = concat.apply(_int_id_from_str)
    assert out.is_unique, "Surrogate key is not unique"
    return out


def _int_id_from_str(s: str) -> int:
    """Use a hash function to convert a string to a unique integer."""
    # hash with md5, subset 16 byte digest to first 8 bytes and cast to int64 because
    # integers are more convenient and we don't need the full 16 bytes for data this
    # small (collision probability is on the order of 1e-12 for 10k items with 64 bits)
    byte_str = s.encode("utf-8")
    hash_digest = hashlib.md5(byte_str).digest()[:8]  # nosec
    # Specify byteorder and signedness just for clarity. I don't *really* care what they
    # are as long as the underlying bytes are unique. But Postgres has no unsigned
    # integer types, so I'll use signed int for consistency between the pre-db and
    # post-db data.
    int_id = int.from_bytes(hash_digest, byteorder="little", signed=True)
    return int_id


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Conduct cleaning on columns of ACP data."""

    def _str_strip(ser):
        return ser.str.strip()

    # clean priority columns. Leave the rest as raw_
    col_transforms: dict[str, Callable] = {
        "raw_status": _col_transform_status,
        "raw_eia_plant_code": _col_transform_eia_plant_code,
        "raw_project_name": _str_strip,
        "raw_phase_name": _str_strip,
        "raw_phase_type": _col_transform_phase_type,
        "raw_developers": _str_strip,
        "raw_owners": _str_strip,
        "raw_iso_rtos": _col_transform_iso_rtos,
        "raw_owner_types": partial(_col_transform_owner_types, full_df=df),
        "raw_mw_total_capacity": partial(_col_transform_mw_total_capacity, full_df=df),
    }
    for raw_col, transform in col_transforms.items():
        new_col_name = raw_col[4:]  # remove 'raw_'
        df[new_col_name] = transform(df[raw_col])
    county_shapes = _extract_census_counties(CENSUS_URI).set_geometry("geometry")
    location_cols = _transform_location_cols(df, county_shapes)
    out = pd.concat((df, location_cols), axis=1)

    # Drop raw_ columns where the transforms did ~zero semantic interpretation
    # Keep any with multi-valued entries, for example.
    cols_to_drop = [
        "raw_status",
        "raw_eia_plant_code",
        "raw_project_name",
        "raw_phase_name",
        "raw_phase_type",
        "raw_developers",
        "raw_owners",
    ]
    # standardize some column names to match existing sources
    rename_dict = {
        "eia_plant_code": "plant_id_eia",  # match PUDL
        "iso_rtos": "iso_region",  # match LBNL
        "phase_type": "resource",  # match LBNL/gridstatus
        "mw_total_capacity": "capacity_mw",  # match PUDL/LBNL/gridstatus
    }
    out.rename(columns=rename_dict, inplace=True)
    out.drop(columns=cols_to_drop, inplace=True)
    return out


def _clean_col_names_and_create_id(df) -> pd.DataFrame:
    """Create surrogate key based on raw columns, then rename and assign ID.

    The surrogate key is created based on raw data and column names.
    Create the surrogate key, then clean column names and specify
    which columns are raw, and assign the surrogate key ID column.
    """
    surrogate_key = _make_surrogate_key(df)  # uses raw column names
    df.columns = _rename_columns(df.columns)
    df = df.rename(columns={"raw_report_date": "report_date"})
    df["proj_id"] = surrogate_key  # assign after renaming columns
    return df


def _transform_acp_projects_current(raw_df: pd.DataFrame) -> pd.DataFrame:
    trans = raw_df.convert_dtypes()
    trans = _clean_col_names_and_create_id(trans)
    out = _clean_columns(trans)

    return out


def _transform_acp_snapshots_to_changelog(
    raw_snapshots_df: pd.DataFrame,
) -> pd.DataFrame:
    """Create a changelog of the ACP data over time."""
    trans_df = raw_snapshots_df.reset_index(drop=True).convert_dtypes()
    trans_df = (
        trans_df.groupby("report_date")
        .apply(_clean_col_names_and_create_id)
        .reset_index(drop=True)
    )
    trans_df = _clean_columns(trans_df)
    trans_df = trans_df.sort_values(by=["proj_id", "report_date"])
    compare_cols = [
        col
        for col in list(trans_df.columns)
        if (col not in ["report_date"] and not col.startswith("raw"))
    ]
    # Shift to compare with previous row within each proj_id
    trans_df["row_hash"] = (
        trans_df[compare_cols]
        .astype(str)
        .apply(
            lambda x: hashlib.sha256(str(tuple(x)).encode("utf-8")).hexdigest(), axis=1
        )
    )
    trans_df["prev_hash"] = trans_df.groupby("proj_id")["row_hash"].shift()
    out_df = trans_df[
        (trans_df.prev_hash.isnull()) | (trans_df.row_hash != trans_df.prev_hash)
    ]
    out_df = out_df.drop(columns=["row_hash", "prev_hash"])
    out_df["valid_until_date"] = out_df.groupby("proj_id")["report_date"].shift(-1)

    return out_df


def transform(raw_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Transform and clean ACP projects data."""
    transformed_dfs = {}
    snapshots = raw_dfs["raw_acp_projects_snapshots"]
    most_recent_report_date = raw_dfs["raw_acp_projects_snapshots"]["report_date"].max()
    most_recent_snapshot = snapshots[
        snapshots["report_date"] == most_recent_report_date
    ]
    transformed_dfs["acp_projects_current"] = _transform_acp_projects_current(
        most_recent_snapshot
    )
    transformed_dfs["acp_changelog"] = _transform_acp_snapshots_to_changelog(
        raw_dfs["raw_acp_projects_snapshots"]
    )

    return transformed_dfs
