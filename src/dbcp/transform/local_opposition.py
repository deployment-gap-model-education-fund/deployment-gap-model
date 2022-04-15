from typing import Dict

import pandas as pd

from dbcp.transform.helpers import add_county_fips_with_backup_geocoding
from pudl.helpers import add_fips_ids as _add_fips_ids


def _extract_years(ser: pd.Series) -> pd.Series:
    """Extract year-like strings from text and summarize with min, max, count.

    The key assumption behind this is that all numbers 1990 - 2029 are interpreted as years
    Also, the purpose of these summaries is really to help users to assume
    earliest_year_mentioned means 'year enacted', which is not always true.

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
                      'min': 'earliest_year_mentioned',
                      'max': 'latest_year_mentioned',
                      'count': 'n_years_mentioned',
                      },
                      inplace=True,
                      )

    only_one_year = summarized.loc[:, 'n_years_mentioned'] == 1
    summarized.loc[only_one_year, 'latest_year_mentioned'] = pd.NA
    summarized = summarized.reindex(index=ser.index, fill_value=pd.NA)
    summarized.loc[:, 'n_years_mentioned'].fillna(0, inplace=True)
    return summarized


def _transform_state_policy(state_policy_df: pd.DataFrame) -> pd.DataFrame:
    """Add FIPS codes and summarize years for state policies

    Args:
        state_policy_df (pd.DataFrame): dataframe of state policies

    Returns:
        pd.DataFrame: dataframe of state policies with additional columns
    """
    state = _add_fips_ids(state_policy_df, county_col='policy').drop(
        columns='county_id_fips')
    year_summaries = _extract_years(state.loc[:, 'policy'])
    state = pd.concat([state, year_summaries], axis=1)
    state.rename(columns={'state': 'raw_state_name'}, inplace=True)
    return state


def _transform_local_ordinances(local_ord_df: pd.DataFrame) -> pd.DataFrame:
    """Standardize locality names, add FIPS codes, and summarize years for local ordinances.

    Args:
        project_df (pd.DataFrame): dataframe of local ordinances

    Returns:
        pd.DataFrame: dataframe of local ordinances with additional columns
    """
    local = local_ord_df.copy()
    for col in local.columns:
        local.loc[:, col] = local.loc[:, col].str.strip()
    # remove straggling words in county names
    local.loc[:, 'locality'] = local.loc[:, 'locality'].str.replace(
        ' Solar$| Wind$| Zoning Ordinance$', '', regex=True)
    # add fips codes to counties (but many names are cities)
    with_fips = add_county_fips_with_backup_geocoding(
        local, locality_col='locality')

    year_summaries = _extract_years(local['ordinance'])
    local = pd.concat([with_fips, year_summaries], axis=1)
    local.rename(columns={'locality': 'raw_locality_name',
                 'state': 'raw_state_name'}, inplace=True)

    return local


def _transform_contested_projects(project_df: pd.DataFrame) -> pd.DataFrame:
    """Add FIPS codes and summarize years for contested projects

    Args:
        project_df (pd.DataFrame): dataframe of contested projects

    Returns:
        pd.DataFrame: dataframe of contested projects with additional columns
    """
    proj = _add_fips_ids(project_df, county_col='description').drop(
        columns='county_id_fips')
    year_summaries = _extract_years(proj.loc[:, 'description'])
    proj = pd.concat([proj, year_summaries], axis=1)
    proj.rename(columns={'state': 'raw_state_name'}, inplace=True)
    return proj


def transform(raw_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    transform_funcs = {
        'state_policy': _transform_state_policy,
        'local_ordinance': _transform_local_ordinances,
        'contested_project': _transform_contested_projects,
    }
    transformed = {key: transform_funcs[key](
        raw_dfs[key]) for key in raw_dfs.keys()}
    return transformed
