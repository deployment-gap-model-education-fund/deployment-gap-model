"""Common transform operations."""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
from joblib import Memory

from dbcp.constants import FIPS_CODE_VINTAGE
from dbcp.helpers import add_fips_ids
from dbcp.transform import geocodio, google_maps

logger = logging.getLogger(__name__)

UNIX_EPOCH_ORIGIN = pd.Timestamp("01/01/1970")
# Excel parser is simplified and will be one day off for dates < 1900/03/01
# The origin is actually 12/31/1899, but because Excel mistakenly thinks
# 1900 was a leap year, I cancel out that error by setting the origin to 12/30/1899.
# See xlrd.xldate.py:xldate_as_datetime for complete implementation.
EXCEL_EPOCH_ORIGIN = pd.Timestamp("12/30/1899")


@dataclass
class MemoryCaches:
    """
    Container for multiple Memory caches.

    Attributes:
        caches: list of Memory caches
    """

    caches: list[Memory]

    def reduce_cache_sizes(self):
        """Reduce the size of all caches."""
        for cache in self.caches:
            cache.reduce_size()

    def clear_caches(self):
        """Clear all caches."""
        for cache in self.caches:
            cache.clear()


GEOCODER_CACHES = MemoryCaches([geocodio.GEOCODER_CACHE, google_maps.GEOCODER_CACHE])


def normalize_multicolumns_to_rows(
    df: pd.DataFrame,
    attribute_columns_dict: Dict[str, Sequence[str]],
    index_cols: Optional[List[str]] = None,
    preserve_original_names=True,
    dropna=True,
) -> pd.DataFrame:
    """Convert a denormalized one-to-many relationship encoded as multiple columns to a row-based table.

    This is essentially the same as pd.melt() except that it handles multiple linked attributes
    such as in the example below. pd.melt() can only convert a single attribute.

    Args:
        df: dataframe with multivalued column(s) encoded as multiple columns
        attribute_columns_dict: dict mapping new value names to a list of
            columns containing that value. If there are multiple such lists, the order of associated
            columns must be the same (eg. if numbered, sorted in same order). See example below.
            index_cols (Optional[List[str]], optional): Columns to use as IDs in original dataframe. If
            None, use existing index. Defaults to None.
        preserve_original_names: Sometimes multicolumn names contain information
            (such as a ranking). If True, keep one of the original column names to preserve this
            information. This assumes associated columns can be identified by a single member (like if
            they share a numbering schema, as in the example below). Defaults to True.
        dropna: Many multicolumns are sparse and produce many empty rows upon
            conversion to long format. If True, drop those rows. Defaults to True.

    Returns:
        pd.DataFrame: one-to-many table

    Example:
    original_df has columns [
        fuel_1, fuel_2, ... , fuel_N,
        capacity_of_fuel_1, capacity_of_fuel_2, ... , capacity_of_fuel_N
        ]
    output_df has columns [fuel, capacity] with N times as many rows.

    create output_df with:
    normalize_multicolumns_to_rows(
        original_df,
        attribute_columns_dict = {
            'fuel': ['fuel_1', 'fuel_2', 'fuel_N'],
            'capacity': ['capacity_of_fuel_1', 'capacity_of_fuel_2', 'capacity_of_fuel_N'],
            <more attribute:column mappings here if needed>,
        },
        preserve_original_names=False
    )
    Note: the lists of fuel_N and capacity_of_fuel_N must be in the same order, or the associations
    will be wrong (fuel_2 with capacity_of_fuel_1, for example). Really it should be a tabular data
    structure rather than multiple independent lists.
    """
    if index_cols is not None:
        df = df.set_index(index_cols)

    new_names = attribute_columns_dict.keys()
    column_groups = attribute_columns_dict.values()
    chunks = []
    for linked_columns in zip(*column_groups):  # Nth value of each list
        rename_dict = dict(zip(linked_columns, new_names))
        chunk = df.loc[:, list(linked_columns)].rename(columns=rename_dict)
        if preserve_original_names:
            # Assumes associated columns can be identified by a single member.
            # For example, (type_1, value_1), (type_2, value_2) share a numbering schema
            chunk["original_group"] = linked_columns[0]
        chunks.append(chunk)

    output: pd.DataFrame = pd.concat(chunks)
    if dropna:
        output.dropna(subset=list(new_names), how="all", inplace=True)

    return output.sort_index().reset_index()


def multiformat_string_date_parser(
    dates: pd.Series, numeric_origin=EXCEL_EPOCH_ORIGIN
) -> pd.Series:
    """Iteratively parse a column of date strings with heterogeneous formatting.

    The LBNL ISO Queue contains a variety of date formats. I couldn't get
    pd.to_datetime() to convert all of them with a single function call because
    it assumes that if the first N rows have the same format, all of them do.
    This solution iteratively calls pd.to_datetime() and inputs the results to .fillna()

    Args:
        date_strings (pd.Series): column of strings to be converted to pd.Timestamp
        parse_excel (bool, optional): Whether to try to interpret remaining strings as excel date offset integers. Defaults to True.

    Returns:
        pd.Series: dates converted to pd.Timestamp
    """
    if not pd.api.types.is_string_dtype(dates):
        raise ValueError(f"Column is not a string dtype. Given {dates.dtype}.")

    # Fill incomplete dates that contain only a year, eg "2020"
    # Conservatively only do this for 4 digit numbers from 1990-2039
    dates = dates.str.replace(
        r"^(199\d|20[0123]\d)$", lambda x: f"07/01/{x.group(1)}", regex=True
    )

    # separate numeric encodings from string encodings
    is_numeric_string = dates.str.isnumeric().fillna(False)
    date_strings = dates.loc[~is_numeric_string]

    # parse strings
    parsed_dates = pd.to_datetime(
        date_strings, infer_datetime_format=True, errors="coerce"
    )
    remaining_nan = parsed_dates.isna().sum()
    while remaining_nan > 0:
        nans = parsed_dates.isna()
        nan_to_dates = pd.to_datetime(
            date_strings[nans], infer_datetime_format=True, errors="coerce"
        )
        parsed_dates = parsed_dates.fillna(nan_to_dates)
        new_remaining_nan = nans.sum()
        if new_remaining_nan == remaining_nan:  # no improvement
            # try specific weird formats
            formats = [
                "%d%b%Y",  # 01Jan2020
                "%m/%y",  # 01/20 or 1/20
                "%b-%y",  # Jan-20
            ]
            for fmt in formats:
                nan_to_dates = pd.to_datetime(
                    date_strings[nans], format=fmt, errors="coerce"
                )
                parsed_dates = parsed_dates.fillna(nan_to_dates)
                nans = parsed_dates.isna()
                if nans.sum() == 0:
                    break
            break
        else:
            remaining_nan = new_remaining_nan

    # handle numeric encodings
    numbers = pd.to_numeric(dates.loc[is_numeric_string], errors="coerce")
    encoded_dates = numeric_offset_date_encoder(numbers, origin=numeric_origin)

    # recombine
    new_dates = pd.concat([parsed_dates, encoded_dates], copy=False).loc[dates.index]
    pd.testing.assert_index_equal(new_dates.index, dates.index)

    return new_dates


def numeric_offset_date_encoder(
    series: pd.Series,
    origin=EXCEL_EPOCH_ORIGIN,
    unit="d",
    roundoff: Optional[str] = None,
) -> pd.Series:
    """Convert column of numeric date offsets (like 45059) to pd.Timestamp.

    Warning: validation is left to the user! Check for unexpected dates.

    Args:
        series (pd.Series): numeric date offsets
        origin (pd.Timestamp): epoch origin, such as 1/1/1970 for unix
        unit (str, optional): pandas frequency string. Defaults to 'd'.
        roundoff (Optional[str], optional): pandas frequency string to round to, such as 'L' for milliseconds. Defaults to None.

    Raises:
        ValueError: if input column is not numeric

    Returns:
        pd.Series: output timestamps
    """
    if len(series) == 0:  # accept empty series
        return series
    if not pd.api.types.is_numeric_dtype(series):
        raise ValueError(f"Series is not numeric. Given {series.dtype}")
    offsets = pd.to_timedelta(series, unit=unit)
    date = offsets + origin
    if roundoff:
        # remove roundoff error to desired precision
        date = date.dt.round(freq=roundoff)
    return date


def parse_dates(series: pd.Series, expected_mean_year=2020) -> pd.Series:
    """Convert a column to pd.Datetime using one of several parsing strategies.

    Numeric columns are interpreted as either Unix or Excel datetime encoding.
    The choice of epoch is determined by which encoding produces dates closer
    to the expected_mean_year. 70 years separate Unix from Excel epochs, so
    this choice is usually clear.

    String columns are passed to the multiformat_string_date_parser.

    Args:
        series (pd.Series): encoded dates
        expected_mean_year (int, optional): year to compare outputs to when
            choosing between Unix and Excel epochs. For numeric columns only.
            Defaults to 2020.

    Returns:
        pd.Series: new column of pd.Datetime
    """
    if pd.api.types.is_numeric_dtype(series):
        unix_dates = numeric_offset_date_encoder(series, origin=UNIX_EPOCH_ORIGIN)
        excel_dates = numeric_offset_date_encoder(series, origin=EXCEL_EPOCH_ORIGIN)
        unix_diff = expected_mean_year - unix_dates.dt.year.mean()
        excel_diff = expected_mean_year - excel_dates.dt.year.mean()
        if abs(unix_diff) < abs(excel_diff):
            return unix_dates
        else:
            return excel_dates
    else:
        # assumes excel epoch when mixed with strings
        return multiformat_string_date_parser(series)


def _geocode_and_add_fips(
    nan_fips: pd.DataFrame, state_col="state", locality_col="county", api="geocodio"
) -> pd.DataFrame:
    """Geocode locality names in a dataframe and add FIPS codes.

    This is used for records that are not assigned a FIPS code by the initial lookup.

    Args:
        nan_fips: dataframe with state and locality columns
        state_col: name of the state column
        locality_col: name of the locality column
        api: name of the geocoding API to use
    Returns:
        dataframe with geocoded locality information
    """
    # Deduplicate on the state and locality columns to minimize API calls
    key_cols = [state_col, locality_col]
    deduped_nan_fips = nan_fips.loc[:, key_cols].drop_duplicates()
    if api == "google":
        geocoding_module = google_maps
    elif api == "geocodio":
        geocoding_module = geocodio
    else:
        raise ValueError(f"Unknown API: {api}")

    deduped_geocoded = geocoding_module._geocode_locality(
        deduped_nan_fips,
        # pass subset to _geocode_locality to maximize chance of a cache hit
        # (this way other columns can change but caching still works)
        state_col=state_col,
        locality_col=locality_col,
    )
    # convert all columns of deduped_geocoded to dtype string
    deduped_geocoded = deduped_geocoded.astype("string")

    # recombine deduped geocoded data with original nan_fips
    geocoded_deduped_nan_fips = pd.concat(
        [deduped_nan_fips[key_cols], deduped_geocoded], axis=1
    )
    index_name = nan_fips.index.name
    index_name = index_name if index_name is not None else "index"
    geocoded = (
        nan_fips.reset_index()
        .merge(geocoded_deduped_nan_fips, on=key_cols, how="left", validate="m:1")
        .set_index(index_name)[deduped_geocoded.columns]
    )

    nan_fips = pd.concat([nan_fips, geocoded], axis=1)
    # add fips using geocoded names
    return add_fips_ids(
        nan_fips,
        state_col=state_col,
        county_col="geocoded_containing_county",
        vintage=FIPS_CODE_VINTAGE,
    )


def add_county_fips_with_backup_geocoding(
    state_locality_df: pd.DataFrame,
    state_col="state",
    locality_col="county",
    debug=False,
) -> pd.DataFrame:
    """Add state and county FIPS codes to a DataFrame with state and locality columns.

    This function is tolerant of mis-spellings and heterogeneous town/city/county types
    because it uses the Google Maps Platform API to re-process initial matching failures.

    Args:
        state_locality_df (pd.DataFrame): dataframe with state and locality columns
        state_col (str, optional): name of the column of state names. Defaults to 'state'.
        locality_col (str, optional): name of the column of locality names. Defaults to 'county'.

    Returns:
        pd.DataFrame: copy of state_locality_df with new columns 'geocoded_locality_name', 'geocoded_locality_type', 'geocoded_containing_county'
    """
    # throw an error if the dataframe is empty
    if state_locality_df.empty:
        raise ValueError("There is no data in this DataFrame to geocode!")

    cols_to_keep = [
        "state_id_fips",
        "county_id_fips",
        "geocoded_locality_name",
        "geocoded_locality_type",
        "geocoded_containing_county",
    ]

    filled_state_locality = state_locality_df.loc[:, [state_col, locality_col]].fillna(
        ""
    )  # copy
    # first try a simple FIPS lookup and split by valid/invalid fips codes
    # The only purpose of this step is to save API calls on the easy ones (most of them)
    add_fips_results = add_fips_ids(
        filled_state_locality,
        state_col=state_col,
        county_col=locality_col,
        vintage=FIPS_CODE_VINTAGE,
    )
    fips_code_is_nan = add_fips_results.loc[:, "county_id_fips"].isna()
    has_fips_code = add_fips_results.loc[~fips_code_is_nan, :].copy()
    # standardize output columns
    has_fips_code["geocoded_locality_name"] = has_fips_code[locality_col]
    has_fips_code["geocoded_locality_type"] = "county"
    has_fips_code["geocoded_locality_type"] = has_fips_code[
        "geocoded_locality_type"
    ].astype("string")
    has_fips_code["geocoded_containing_county"] = has_fips_code[locality_col]

    # if all records have a FIPS code, no need to geocoded. Combine with original dataframe
    if len(has_fips_code) == len(state_locality_df):
        return pd.concat([state_locality_df, has_fips_code[cols_to_keep]], axis=1)

    # geocode the lookup failures - they are often city/town names (instead of counties) or simply mis-spelled
    nan_fips = add_fips_results.loc[fips_code_is_nan, :].copy()

    # Compare google and geocodio results
    geocodio_df = _geocode_and_add_fips(
        nan_fips, state_col=state_col, locality_col=locality_col, api="geocodio"
    )
    if debug:
        google_df = _geocode_and_add_fips(
            nan_fips, state_col=state_col, locality_col=locality_col, api="google"
        )

        # combine the two geocoded dataframes
        comp = geocodio_df.merge(
            google_df,
            left_index=True,
            right_index=True,
            how="outer",
            validate="1:1",
            suffixes=("_geocodio", "_google"),
        )

        county_eq = comp.county_id_fips_geocodio.eq(comp.county_id_fips_google)
        logger.info("---------------------")
        logger.info(
            f"---- pct of geocoded fip failures that don't match: {(~county_eq).sum() / len(comp)}"
        )
        logger.info(
            f"---- pct of all records that don't have the same county: {(~county_eq).sum() / len(state_locality_df)}"
        )
        logger.info("---------------------")

    filled_fips = geocodio_df

    # recombine and restore row order
    recombined = pd.concat([has_fips_code, filled_fips], axis=0).loc[
        state_locality_df.index, cols_to_keep
    ]

    # attach to original df
    out = pd.concat([state_locality_df, recombined], axis=1)

    return out


def replace_value_with_count_validation(
    df: pd.DataFrame,
    col: str,
    val_to_replace: Any,
    replacement: Any,
    expected_count: int,
) -> None:
    """Manually replace values, but with a minimal form of validation to guard against future changes.

    Args:
        df (pd.DataFrame): the source dataframe
        col (str): the name of the column containing values to replace
        val_to_replace (Any): value to replace
        replacement (Any): replacement value
        expected_count (int): known number of replacements to make

    Raises:
        ValueError: if expected count of replacements does not match observed count
    """
    matches = df.loc[:, col] == val_to_replace
    observed_count = matches.sum()
    if observed_count != expected_count:
        raise ValueError(
            f"Expected count ({expected_count}) of {val_to_replace} "
            f"does not match observed count ({observed_count})"
        )

    df.loc[matches, col] = replacement
    return


def bedford_addfips_fix(df, state_col="state", county_col="county") -> None:
    """Workaround to fix addfips data problem.

    addfips includes Bedford City, VA in its county database even though it was
    absorbed by Bedford County, VA in 2013. Its FIPS code no longer exists.
    """
    # workaround for addfips Bedford, VA problem
    is_va = df[state_col].str.lower().isin({"va", "virginia"})
    is_bedford = df[county_col].str.lower().str.startswith("bedford")
    df.loc[is_va & is_bedford, county_col] = "Bedford County"
    return
