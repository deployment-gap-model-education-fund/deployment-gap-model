"""Common transform operations."""
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from joblib import Memory

from dbcp.constants import FIPS_CODE_VINTAGE
from dbcp.transform.geocoding import GoogleGeocoder
from pudl.helpers import add_fips_ids as _add_fips_ids

UNIX_EPOCH_ORIGIN = pd.Timestamp('01/01/1970')
# Excel parser is simplified and will be one day off for dates < 1900/03/01
# The origin is actually 12/31/1899, but because Excel mistakenly thinks
# 1900 was a leap year, I cancel out that error by setting the origin to 12/30/1899.
# See xlrd.xldate.py:xldate_as_datetime for complete implementation.
EXCEL_EPOCH_ORIGIN = pd.Timestamp('12/30/1899')

try:  # docker path
    # 3 directories above current module
    geocoder_local_cache = Path("/app/data/geocoder_cache")
    assert geocoder_local_cache.exists()
except AssertionError:  # local path
    # 4 directories above current module
    geocoder_local_cache = Path(
        __file__).resolve().parents[3] / "data/geocoder_cache"
    assert geocoder_local_cache.exists()
# cache needs to be accessed outside this module to call .clear()
# limit cache size to 100 KB, keeps most recently accessed first
GEOCODER_CACHE = Memory(location=geocoder_local_cache, bytes_limit=100000)


def normalize_multicolumns_to_rows(
    df: pd.DataFrame,
    attribute_columns_dict: Dict[str, List[str]],
    index_cols: Optional[List[str]] = None,
    preserve_original_names=True,
    dropna=True
) -> pd.DataFrame:
    """Convert a denormalized one-to-many relationship encoded as multiple columns to a row-based table.

    This is essentially the same as pd.melt() except that it handles multiple linked attributes
    such as in the example below. pd.melt() can only convert a single attribute.

    Args:
        df (pd.DataFrame): dataframe with multivalued column(s) encoded as multiple columns
        attribute_columns_dict (Dict[str,List[str]]): dict mapping new value names to a list of
        columns containing that value. If there are multiple such lists, the order of associated
        columns must be the same (eg. if numbered, sorted in same order). See example below.
        index_cols (Optional[List[str]], optional): Columns to use as IDs in original dataframe. If
        None, use existing index. Defaults to None.
        preserve_original_names (bool, optional): Sometimes multicolumn names contain information
        (such as a ranking). If True, keep one of the original column names to preserve this
        information. This assumes associated columns can be identified by a single member (like if
        they share a numbering schema, as in the example below). Defaults to True.
        dropna (bool, optional): Many multicolumns are sparse and produce many empty rows upon
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
            chunk['original_group'] = linked_columns[0]
        chunks.append(chunk)

    output: pd.DataFrame = pd.concat(chunks)
    if dropna:
        output.dropna(subset=list(new_names), how='all', inplace=True)

    return output.sort_index().reset_index()


def multiformat_string_date_parser(dates: pd.Series, numeric_origin=EXCEL_EPOCH_ORIGIN) -> pd.Series:
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

    # separate numeric encodings from string encodings
    is_numeric_string = dates.str.isnumeric().fillna(False)
    date_strings = dates.loc[~is_numeric_string]

    # parse strings
    parsed_dates = pd.to_datetime(
        date_strings, infer_datetime_format=True, errors='coerce')
    remaining_nan = parsed_dates.isna().sum()
    while True:
        nans = parsed_dates.isna()
        nan_to_dates = pd.to_datetime(
            date_strings[nans], infer_datetime_format=True, errors='coerce')
        parsed_dates = parsed_dates.fillna(nan_to_dates)
        new_remaining_nan = nans.sum()
        if new_remaining_nan == remaining_nan:  # no improvement
            break
        else:
            remaining_nan = new_remaining_nan

    # handle numeric encodings
    numbers = pd.to_numeric(dates.loc[is_numeric_string], errors='coerce')
    encoded_dates = numeric_offset_date_encoder(numbers, origin=numeric_origin)

    # recombine
    new_dates = pd.concat([parsed_dates, encoded_dates],
                          copy=False).loc[dates.index]
    pd.testing.assert_index_equal(new_dates.index, dates.index)

    return new_dates


def numeric_offset_date_encoder(series: pd.Series, origin=EXCEL_EPOCH_ORIGIN, unit='d', roundoff: Optional[str] = None) -> pd.Series:
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
        raise ValueError(f'Series is not numeric. Given {series.dtype}')
    offsets = pd.to_timedelta(series, unit=unit)
    date = offsets + origin
    if roundoff:
        # remove roundoff error to desired precision
        date = date.dt.round(freq=roundoff)
    return date


def parse_dates(series: pd.Series, expected_mean_year=2020) -> pd.Series:
    """convert a column to pd.Datetime using one of several parsing strategies.

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
        unix_dates = numeric_offset_date_encoder(
            series, origin=UNIX_EPOCH_ORIGIN)
        excel_dates = numeric_offset_date_encoder(
            series, origin=EXCEL_EPOCH_ORIGIN)
        unix_diff = (expected_mean_year - unix_dates.dt.year.mean())
        excel_diff = (expected_mean_year - excel_dates.dt.year.mean())
        if abs(unix_diff) < abs(excel_diff):
            return unix_dates
        else:
            return excel_dates
    else:
        # assumes excel epoch when mixed with strings
        return multiformat_string_date_parser(series)


def _geocode_row(ser: pd.Series, client: GoogleGeocoder, state_col='state', locality_col='county') -> List[str]:
    """function to pass into pandas df.apply() to geocode state/locality pairs

    Args:
        ser (pd.Series): a row of a larger dataframe to geocode
        client (GoogleGeocoder): client for Google Maps Platform API
        state_col (str, optional): name of the column of state names. Defaults to 'state'.
        locality_col (str, optional): name of the column of locality names. Defaults to 'county'.

    Returns:
        List[str]: geocoded_locality_name, geocoded_locality_type, and geocoded_containing_county
    """
    client.geocode_request(name=ser[locality_col], state=ser[state_col])
    return client.describe()


@GEOCODER_CACHE.cache()
def _geocode_locality(state_locality_df: pd.DataFrame, state_col='state', locality_col='county') -> pd.DataFrame:
    """Use Google Maps Platform API to look up information about state/locality pairs in a dataframe.

    Args:
        state_locality_df (pd.DataFrame): dataframe with state and locality columns
        state_col (str, optional): name of the column of state names. Defaults to 'state'.
        locality_col (str, optional): name of the column of locality names. Defaults to 'county'.

    Returns:
        pd.DataFrame: new columns 'geocoded_locality_name', 'geocoded_locality_type', 'geocoded_containing_county'
    """
    # NOTE: the purpose of the cache decorator is primarily to
    # reduce API calls during development. A secondary benefit is to reduce
    # execution time due to slow synchronous requests.
    # That's why this is persisted to disk with joblib, not in memory with LRU_cache or something.
    # Because it is on disk, caching the higher level dataframe function causes less IO overhead
    # than caching individual API calls would.
    # Because the entire input dataframe must be identical to the cached version, I
    # recommend subsetting the dataframe to only state_col and locality_col when calling
    # this function. That allows other, unrelated columns to change but still use the geocode cache.
    geocoder = GoogleGeocoder()
    new_cols = state_locality_df.apply(
        _geocode_row, axis=1, result_type='expand', client=geocoder, state_col=state_col, locality_col=locality_col)
    new_cols.columns = ['geocoded_locality_name',
                        'geocoded_locality_type', 'geocoded_containing_county']
    return new_cols


def add_county_fips_with_backup_geocoding(state_locality_df: pd.DataFrame, state_col='state', locality_col='county') -> pd.DataFrame:
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
    filled_state_locality = state_locality_df.loc[:, [
        state_col, locality_col]].fillna('')  # copy
    # first try a simple FIPS lookup and split by valid/invalid fips codes
    # The only purpose of this step is to save API calls on the easy ones (most of them)
    with_fips = _add_fips_ids(
        filled_state_locality, state_col=state_col, county_col=locality_col, vintage=FIPS_CODE_VINTAGE)
    fips_is_nan = with_fips.loc[:, 'county_id_fips'].isna()
    if not fips_is_nan.any():
        # standardize output columns
        with_fips['geocoded_locality_name'] = with_fips[locality_col]
        with_fips['geocoded_locality_type'] = 'county'
        with_fips['geocoded_containing_county'] = with_fips[locality_col]
        return with_fips

    good_fips = with_fips.loc[~fips_is_nan, :].copy()
    # standardize output columns
    good_fips['geocoded_locality_name'] = good_fips[locality_col]
    good_fips['geocoded_locality_type'] = 'county'
    good_fips['geocoded_containing_county'] = good_fips[locality_col]

    # geocode the lookup failures - they are often city/town names (instead of counties) or simply mis-spelled
    nan_fips = with_fips.loc[fips_is_nan, :].copy()
    geocoded = _geocode_locality(
        nan_fips.loc[:, [state_col, locality_col]],
        # pass subset to _geocode_locality to maximize chance of a cache hit
        # (this way other columns can change but caching still works)
        state_col=state_col,
        locality_col=locality_col
    )
    nan_fips = pd.concat([nan_fips, geocoded], axis=1)
    # add fips using geocoded names
    filled_fips = _add_fips_ids(nan_fips, state_col=state_col,
                                county_col='geocoded_containing_county', vintage=FIPS_CODE_VINTAGE)

    # recombine and restore row order
    cols_to_keep = ['state_id_fips', 'county_id_fips',
                    'geocoded_locality_name', 'geocoded_locality_type', 'geocoded_containing_county', ]
    recombined = pd.concat([good_fips, filled_fips],
                           axis=0).loc[state_locality_df.index, cols_to_keep]

    # attach to original df
    out = pd.concat([state_locality_df, recombined], axis=1)

    return out


def replace_value_with_count_validation(df: pd.DataFrame, col: str, val_to_replace: Any, replacement: Any, expected_count: int) -> None:
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
