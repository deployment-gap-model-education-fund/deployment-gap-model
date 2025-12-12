"""Transform functions for local opposition data."""

from typing import Dict

import pandas as pd

from dbcp.constants import FIPS_CODE_VINTAGE
from dbcp.helpers import add_fips_ids
from dbcp.transform.helpers import add_county_fips_with_backup_geocoding


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
    years = ser.str.extractall(r"(?P<year>199\d|20[012]\d)").squeeze()
    years = pd.to_numeric(years)  # convert string years to ints

    summarized = years.groupby(level=0).agg(["min", "max", "count"])
    summarized = summarized.astype(pd.Int16Dtype())
    summarized.rename(
        columns={
            "min": "earliest_year_mentioned",
            "max": "latest_year_mentioned",
            "count": "n_years_mentioned",
        },
        inplace=True,
    )

    only_one_year = summarized.loc[:, "n_years_mentioned"] == 1
    summarized.loc[only_one_year, "latest_year_mentioned"] = pd.NA
    summarized = summarized.reindex(index=ser.index, fill_value=pd.NA)
    summarized.loc[:, "n_years_mentioned"].fillna(0, inplace=True)
    return summarized


def _transform_state_policy(state_policy_df: pd.DataFrame) -> pd.DataFrame:
    """Add FIPS codes and summarize years for state policies.

    Args:
        state_policy_df (pd.DataFrame): dataframe of state policies

    Returns:
        pd.DataFrame: dataframe of state policies with additional columns
    """
    state = add_fips_ids(
        state_policy_df, county_col="policy", vintage=FIPS_CODE_VINTAGE
    ).drop(columns="county_id_fips")
    year_summaries = _extract_years(state.loc[:, "policy"])
    state = pd.concat([state, year_summaries], axis=1)
    state.rename(columns={"state": "raw_state_name"}, inplace=True)
    return state


def _transform_state_notes(state_notes_df: pd.DataFrame) -> pd.DataFrame:
    """Add FIPS codes and summarize years for state notes.

    Args:
        state_notes_df (pd.DataFrame): dataframe of state notes

    Returns:
        pd.DataFrame: dataframe of state notes with additional columns
    """
    # currently same transform as policy.
    # Just rename a column for compatibility, then rename it back.
    notes = state_notes_df.rename(columns={"notes": "policy"})
    notes = _transform_state_policy(notes)
    notes.rename(columns={"policy": "notes"}, inplace=True)
    return notes


def _transform_local_ordinances(local_ord_df: pd.DataFrame) -> pd.DataFrame:
    """Standardize locality names, add FIPS codes, and summarize years for local ordinances.

    Args:
        project_df (pd.DataFrame): dataframe of local ordinances

    Returns:
        pd.DataFrame: dataframe of local ordinances with additional columns
    """
    local = local_ord_df.copy()
    string_cols = [
        col for col in local.columns if pd.api.types.is_object_dtype(local.loc[:, col])
    ]
    for col in string_cols:
        local.loc[:, col] = local.loc[:, col].str.strip()

    # TODO: check if Saratoga County still needs a correction
    # when geocodio-library-python goes in
    # manual corrections
    location_corrections = {
        "Batavia Township (Clermont County)": "Branch County",
        "Town of Albion (Kennebec County)": "Albion (Kennebec County)",
        "Town of Lovell (Oxford County)": "Lovell (Oxford County)",
        "Town of Charlton (Worcester County)": "Charlton (Worcester County)",
        "City of Owasso (Rogers and Tulsa Counties)": "Owasso (Rogers and Tulsa Counties)",
        "City of Burleson (Tarrant and Johnson Counties)": "Burleson (Tarrant and Johnson Counties)",
        "Montrose City (Genesee County)": "Montrose (Genesee County)",
        "Genoa Township (Livingston County)": "Livingston County",
        "Maple Valley Township (Montcalm County)": "Montcalm County",
        "Ellington Township (Tuscola County)": "Tuscola County",
        "Almer Township (Tuscola County)": "Tuscola County",
        "Beaver Township (Bay County)": "Bay County",
        "Matteson Township (Branch County)": "Branch County",
        "Monitor Township (Bay County)": "Bay County",
        "Town of Porter (Niagara County)": "Niagara County",
        "Town of Ballston (Saratoga County)": "Saratoga County",
    }
    raw_locality = local["locality"].copy()
    local.loc[:, "locality"].replace(location_corrections, inplace=True)

    # Remove (Count Name) from localities because geocodio performs better with just the locality name
    local["locality"] = local["locality"].str.replace(r"\s?\(.*?\)", "", regex=True)

    # Remove "City of" and "Town of" prefixes from localities that have them
    # Geocodio thinks these prefixes are street names
    local["locality"] = local["locality"].str.replace(
        r"^(City of|Town of) ", "", regex=True
    )

    # add fips codes to counties (but many names are cities)
    with_fips = add_county_fips_with_backup_geocoding(local, locality_col="locality")

    # undo locality corrections so we can view the raw data
    with_fips.loc[:, "locality"] = raw_locality

    year_summaries = _extract_years(local["ordinance_text"])
    local = pd.concat([with_fips, year_summaries], axis=1)
    local.rename(
        columns={"locality": "raw_locality_name", "state": "raw_state_name"},
        inplace=True,
    )
    _validate_ordinances(local)

    return local


def _transform_contested_projects(project_df: pd.DataFrame) -> pd.DataFrame:
    """Add FIPS codes and summarize years for contested projects.

    Args:
        project_df (pd.DataFrame): dataframe of contested projects

    Returns:
        pd.DataFrame: dataframe of contested projects with additional columns
    """
    # this should really use geocoding, but we don't use this data so I didn't bother.
    proj = add_fips_ids(project_df, county_col="description").drop(
        columns="county_id_fips"
    )
    year_summaries = _extract_years(proj.loc[:, "description"])
    proj = pd.concat([proj, year_summaries], axis=1)
    proj.rename(columns={"state": "raw_state_name"}, inplace=True)
    return proj


def transform(raw_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Transform local opposition data."""
    transform_funcs = {
        "state_policy": _transform_state_policy,
        "state_notes": _transform_state_notes,
        "local_ordinance": _transform_local_ordinances,
        "contested_project": _transform_contested_projects,
    }
    transformed = {key: transform_funcs[key](raw_dfs[key]) for key in raw_dfs.keys()}
    return transformed


def _validate_ordinances(ordn: pd.DataFrame) -> None:
    assert (
        ordn.duplicated(subset=["raw_state_name", "raw_locality_name"]).sum() == 0
    ), "Duplicate ordinance locations."
    assert ordn["county_id_fips"].isna().sum() == 0, "Missing FIPS codes."
    assert (
        ordn["geocoded_locality_name"].str.contains(r"[0-9]", regex=True).sum() == 0
    ), "Geocoded locality names contain numbers."
    assert (
        ordn["geocoded_locality_type"].isna().sum() == 0
    ), "Missing geocoded locality types."


if __name__ == "__main__":
    # debugging entry point
    from pathlib import Path

    from dbcp.extract.local_opposition import ColumbiaDocxParser

    source_path = Path(
        "/app/data/raw/2023.05.30 Opposition to Renewable Energy Facilities - FINAL.docx"
    )
    extractor = ColumbiaDocxParser()
    extractor.load_docx(source_path)
    docx_dfs = extractor.extract()

    # Transform
    transformed_dfs = transform(docx_dfs)
