from typing import Dict, List
import pandas as pd

from pudl.helpers import add_fips_ids as _add_fips_ids

from dbcp.transform.geocoding import GoogleGeocoder


def _extract_years(ser: pd.Series) -> pd.Series:
    """Extract year-like strings from text and summarize with min, max, count.

    There are two key assumptions behind this, so interpret with caution:
    * all numbers 1990 - 2029 are interpreted as years
    * the lowest year is assumed to be year_enacted

    Args:
        ser (pd.Series): string column with policy descriptions

    Returns:
        pd.Series: summary dataframe ready to pd.concat() with input series
    """
    years = ser.str.extractall(r'(?P<year>199\d|2[01][012]\d)').squeeze()
    years = pd.to_numeric(years)  # convert string years to ints

    summarized = years.groupby(level=0).agg(['min', 'max', 'count'])
    summarized = summarized.astype(pd.Int16Dtype())
    summarized.rename(columns={
                      'min': 'year_enacted',
                      'max': 'year_last_updated',
                      'count': 'n_years_mentioned',
                      },
                      inplace=True,
                      )

    only_one_year = summarized.loc[:, 'n_years_mentioned'] == 1
    summarized.loc[only_one_year, 'year_last_updated'] = pd.NA
    summarized = summarized.reindex(index=ser.index, fill_value=pd.NA)
    summarized.loc[:, 'n_years_mentioned'].fillna(0, inplace=True)
    return summarized


def _transform_state_policy(state_policy_df: pd.DataFrame) -> pd.DataFrame:
    year_summaries = _extract_years(state_policy_df['policy'])
    return pd.concat([state_policy_df, year_summaries], axis=1)


def _transform_local_ordinances(local_ord_df: pd.DataFrame) -> pd.DataFrame:
    local = local_ord_df.copy()
    for col in local.columns:
        local.loc[:, col] = local.loc[:, col].str.strip()
    year_summaries = _extract_years(local['ordinance'])
    transformed = pd.concat([local, year_summaries], axis=1)

    # remove straggling words in county names
    transformed.loc[:, 'locality'] = transformed.loc[:, 'locality'].str.replace(
        ' Solar$| Wind$| Zoning Ordinance$', '', regex=True)
    # add fips codes to counties (but many names are cities)
    transformed = _add_fips_ids(
        transformed, county_col='locality', vintage=2020)
    no_fips = transformed.loc[transformed.loc[:, 'county_id_fips'].isna(), :]

    return transformed


def _geocode_row(ser: pd.Series, client: GoogleGeocoder) -> List[str]:
    client.geocode_request(name=ser['locality'], state=ser['state'])
    return client.describe()


def geocode_locality(local_ord_subset: pd.DataFrame) -> pd.DataFrame:
    geocoder = GoogleGeocoder()
    new_cols = local_ord_subset.apply(
        _geocode_row, axis=1, result_type='expand', client=geocoder)
    new_cols.columns = ['object_name', 'object_type', 'containing_county']
    return new_cols


def transform(raw_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    raise NotImplementedError
