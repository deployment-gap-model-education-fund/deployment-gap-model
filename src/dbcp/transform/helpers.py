"""Common transform operations."""
from typing import Optional, List, Dict

import pandas as pd


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
            'capacity': ['capacity_of_fuel_1', 'capacity_of_fuel_2', 'capacity_of_fuel_N']
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

    output = pd.concat(chunks)
    if dropna:
        output = output.dropna(how='all')

    return output.reset_index()


def multiformat_date_parser(date_strings: pd.Series, parse_excel=True) -> pd.Series:
    """Iteratively parse a column of date strings with heterogeneous formatting.

    The LBNL ISO Queue contains a variety of date formats. I couldn't get
    pd.to_datetime() to convert all of them with a single function call because
    it assumes that if the first X have the same format, all of them do.
    This solution iteratively calls pd.to_datetime() and inputs the results to .fillna()

    Args:
        date_strings (pd.Series): column of strings to be converted to pd.Timestamp
        parse_excel (bool, optional): Whether to try to interpret remaining strings as excel date offset integers. Defaults to True.

    Returns:
        pd.Series: dates converted to pd.Timestamp
    """
    dates = pd.to_datetime(
        date_strings, infer_datetime_format=True, errors='coerce')
    remaining_nan = dates.isna().sum()
    while True:
        nan_to_dates = pd.to_datetime(
            date_strings[dates.isna()], infer_datetime_format=True, errors='coerce')
        dates = dates.fillna(nan_to_dates)
        new_remaining_nan = dates.isna().sum()
        if new_remaining_nan == remaining_nan:  # no improvement
            break
        else:
            remaining_nan = new_remaining_nan

    # finally, interpret strings like '45049' as excel date offsets
    if parse_excel:
        maybe_excel_ints = date_strings[dates.isna()]
        parsed_excel_dates = excel_date_parser(maybe_excel_ints)
        dates = dates.fillna(parsed_excel_dates)
    return dates


def excel_date_parser(date_strings: pd.Series) -> pd.Series:
    """Convert column of excel date offsets (like '45059') to pd.Timestamp.

    Warning: validation is left to the user! Check for unexpected dates.

    Args:
        date_strings (pd.Series): column of strings to be converted

    Returns:
        pd.Series: successful timestamp conversions (NaNs dropped)
    """
    excel_origin = pd.Timestamp('01/01/1900')
    numbers = pd.to_numeric(date_strings, errors='coerce').dropna()
    offsets = pd.to_timedelta(numbers, unit='day')
    return offsets + excel_origin
