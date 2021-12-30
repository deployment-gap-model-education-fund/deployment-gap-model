"""Common transform operations."""
from typing import Optional, List, Dict

import pandas as pd

UNIX_EPOCH_ORIGIN = pd.Timestamp('01/01/1970')
# Excel parser is simplified and will be one day off for dates < 1900/03/01
# The origin is actually 12/31/1899, but because Excel mistakenly thinks
# 1900 was a leap year, I cancel out that error by setting the origin to 12/30/1899.
# See xlrd.xldate.py:xldate_as_datetime for complete implementation.
EXCEL_EPOCH_ORIGIN = pd.Timestamp('12/30/1899')


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
