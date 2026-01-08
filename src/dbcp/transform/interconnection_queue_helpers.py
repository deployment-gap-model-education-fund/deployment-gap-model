"""Define common functions that are used for both FYI and LBNL transforms.

The LBNL and FYI tables are structured similarly and go through similar transforms.
This module provides a dedicated place for methods that are shared between these two
datasets without cluttering the general ``helpers`` module.
"""

import pandas as pd

from dbcp.constants import QUEUE_RESOURCE_DICT
from dbcp.transform.helpers import parse_dates


def add_actionable_and_nearly_certain_classification(
    queue: pd.DataFrame, status_col: str
) -> pd.DataFrame:
    """Add columns is_actionable and is_nearly_certain that classify each project.

    This model was created by a consultant in Excel and translated to python.
    """
    if (
        queue[status_col]
        .isin(
            {
                "Facilities Study",
                "Feasability Study",
            }
        )
        .any()
    ):
        raise ValueError("This function only applies to harmonized IA statuses.")
    # the following sets were manually defined by a consultant
    actionable_ia_statuses = {
        "Facility Study",
        "System Impact Study",
        "Phase 4 Study",
        "IA Pending",
    }
    nearly_certain_ia_statuses = {
        "Construction",
        "IA Executed",
        "Operational",
    }
    queue["is_actionable"] = (
        queue[status_col].isin(actionable_ia_statuses).fillna(False)
    )
    queue["is_nearly_certain"] = (
        queue[status_col].isin(nearly_certain_ia_statuses).fillna(False)
    )

    return queue


def clean_resource_type(
    resource_df: pd.DataFrame, resource_dict: dict = QUEUE_RESOURCE_DICT
) -> pd.DataFrame:
    """Standardize resource types used throughout iso queue tables.

    Args:
        resource_df (pd.DataFrame): normalized lbnl/fyi ISO queue resource df.

    Returns:
        pd.DataFrame: A copy of the resource df with a new columns for cleaned resource
            types.

    """
    resource_df = resource_df.copy()
    # Modify QUEUE_RESOURCE DICT for mapping
    long_dict = {}
    for clean_name, code_type_dict in resource_dict.items():
        long_dict[clean_name] = clean_name
        for code in code_type_dict["codes"]:
            long_dict[code] = clean_name
    # Map clean resource values into new column
    resource_df["resource_clean"] = resource_df["resource"].fillna("Unknown")
    resource_df["resource_clean"] = resource_df["resource_clean"].map(long_dict)
    unmapped = resource_df["resource_clean"].isna()
    if unmapped.sum() != 0:
        debug = resource_df.loc[unmapped, "resource"].value_counts()
        raise AssertionError(f"Unmapped resource types: {debug}")
    return resource_df


def fyi_manual_county_state_name_fill_ins(location_df: pd.DataFrame) -> pd.DataFrame:
    """Manually fill in some county and state pairs in FYI that are wrong or missing.

    There are some projects which are missing county/state information in FYI
    but have that location information in the GridStatus/LBNL data. This fills
    in the missing county/state pairs in FYI with manually compiled fill-ins
    copied over from GridStatus/LBNL. The FYI data update notebook should find
    any new fill-ins that aren't recorded here. New fill-ins need to be copied over
    when they arise and projects that are no longer in the queue should be taken
    out of this list (there's a logging warning that says when one of these fill in
    projects are no longer in the queue.)
    """
    manual_fill_ins = [
        ["wapa-rocky-mountain-region-2019-g2", "Jackson", "Colorado"],
        ["wapa-rocky-mountain-region-2023-g7", "Jackson", "Colorado"],
        ["pjm-ag1-471", "Wayne", "Kentucky"],
        ["miso-j2729", "West Baton Rouge", "Louisiana"],
        ["wapa-rocky-mountain-region-2008-g9", "Yuma", "Colorado"],
        ["wapa-rocky-mountain-region-2017-g2", "Banner", "Nebraska"],
        ["wapa-rocky-mountain-region-2018-g6", "San Juan", "New Mexico"],
        ["wapa-rocky-mountain-region-2022-g7", "Weld", "Colorado"],
        ["wapa-rocky-mountain-region-2023-g1", "Scotts Bluff", "Nebraska"],
        ["wapa-rocky-mountain-region-2023-g10-1", "Coconino", "Arizona"],
        ["wapa-rocky-mountain-region-2023-g2", "Albany", "Wyoming"],
        ["wapa-rocky-mountain-region-2023-g5-2", "Coconino", "Arizona"],
        ["wapa-rocky-mountain-region-2024-g3", "Weld", "Colorado"],
        ["wapa-rocky-mountain-region-2024-g4", "Morgan", "Colorado"],
    ]
    manual_fill_ins_df = pd.DataFrame(
        manual_fill_ins,
        columns=["project_id", "county", "state"],
    )
    locs = location_df.copy()
    # It's necessary to do an outer merge here because a project
    # that has a null county and state name won't show up
    # in the locations table. We still want to manually fill in
    # location information for those projects. Later we'll check
    # to make sure they also show up in the projects table.
    locs = locs.merge(
        manual_fill_ins_df,
        how="outer",
        on=["project_id"],
    )
    locs.loc[:, "raw_county_name"] = locs.loc[:, "county"].fillna(
        locs.loc[:, "raw_county_name"]
    )
    locs.loc[:, "raw_state_name"] = locs.loc[:, "state"].fillna(
        locs.loc[:, "raw_state_name"]
    )
    locs = locs.drop(["county", "state"], axis=1)

    return locs


def manual_county_state_name_fixes(location_df: pd.DataFrame) -> pd.DataFrame:
    """Fix around 20 incorrect county, state names.

    Args:
        location_df (pd.DataFrame): normalized ISO locations
    Returns:
        pd.DataFrame: copy of location_df with more correct county, state pairs

    """
    # TODO: of the 77 null county_id_fips, about half of them have missing state
    # values that could probably be inferred by appending "county" to the
    # raw_county_name and extracting the geocoded state, if unique.
    manual_county_state_name_fixes = [
        ["skamania", "or", "skamania", "wa"],
        ["coos & curry", "or", "coos", "or"],
        ["coos & curry", "", "coos", "or"],
        ["lake", "or", "lake county", "or"],
        ["franklin-clinton", "ny", "franklin", "ny"],
        ["san juan", "az", "san juan", "nm"],
        ["hidalgo", "co", "hidalgo", "nm"],
        ["coconino", "co", "coconino", "az"],
        ["antelope & wheeler", "ne", "antelope", "ne"],
        ["linden", "ny", "union", "nj"],
        ["church", "nv", "churchill", "nv"],
        ["churchill/pershing", "ca", "churchill", "nv"],
        ["shasta/trinity", "ca", "shasta", "ca"],
        ["san benito", "nv", "san benito", "ca"],
        ["frqanklin", "me", "franklin", "me"],
        ["logan,menard", "il", "logan", "il"],
        ["clarke", "in", "clark", "il"],
        ["lincoln", "co", "lincoln county", "co"],
        ["new york-nj", "ny", "new york", "ny"],
        ["peneobscot/washington", "me", "penobscot", "me"],
        ["delaware (ok)", "ok", "delaware", "ok"],
        # workaround for bug in addfips library.
        # See https://github.com/fitnr/addfips/issues/8
        ["dryer", "tn", "dyer", "tn"],
        ["churchill", "ca", "churchill", "nv"],
        ["pershing", "ca", "pershing", "nv"],
    ]
    manual_county_state_name_fixes = pd.DataFrame(
        manual_county_state_name_fixes,
        columns=["raw_county_name", "raw_state_name", "clean_county", "clean_state"],
    )

    locs = location_df.copy()
    locs.loc[:, "raw_county_name"] = locs.loc[:, "raw_county_name"].str.lower()
    locs.loc[:, "raw_state_name"] = locs.loc[:, "raw_state_name"].str.lower()
    locs = locs.merge(
        manual_county_state_name_fixes,
        how="left",
        on=["raw_county_name", "raw_state_name"],
    )
    locs.loc[:, "raw_county_name"] = locs.loc[:, "clean_county"].fillna(
        locs.loc[:, "raw_county_name"]
    )
    locs.loc[:, "raw_state_name"] = locs.loc[:, "clean_state"].fillna(
        locs.loc[:, "raw_state_name"]
    )
    locs = locs.drop(["clean_county", "clean_state"], axis=1)

    return locs


def normalize_point_of_interconnection(ser: pd.Series) -> pd.Series:
    """String normalization for point_of_interconnection.

    Essentially a poor man's bag-of-words model.
    """
    out = (
        ser.astype("string")
        .str.lower()
        .str.replace("-", " ")
        .str.replace(r"(?:sub)station|kv| at |tbd", "", regex=True)
        .fillna("")
    )
    out = pd.Series(  # make permutation invariant by sorting
        [" ".join(sorted(x)) for x in out.str.split()],
        index=out.index,
        dtype="string",
    ).str.strip()
    out.replace("", pd.NA, inplace=True)
    return out


def parse_date_columns(queue: pd.DataFrame) -> None:
    """Identify date columns and parse them to pd.Timestamp.

    Original (unparsed) date columns are preserved but with the suffix '_raw'.

    Args:
        queue (pd.DataFrame): an LBNL ISO queue dataframe
    """
    date_cols = [
        col
        for col in queue.columns
        if (
            (col.startswith("date_") or col.endswith("_date"))
            # datetime columns don't need parsing
            and not pd.api.types.is_datetime64_any_dtype(queue.loc[:, col])
        )
    ]

    # add _raw suffix
    rename_dict: dict[str, str] = dict(
        zip(date_cols, [col + "_raw" for col in date_cols])
    )
    queue.rename(columns=rename_dict, inplace=True)

    for date_col, raw_col in rename_dict.items():
        if pd.api.types.is_object_dtype(queue.loc[:, raw_col]):
            queue[raw_col] = queue[raw_col].astype(str)
        new_dates = parse_dates(queue.loc[:, raw_col])
        # set obviously bad values to null
        # This is designed to catch NaN values improperly encoded by Excel to 1899 or 1900
        bad = new_dates.dt.year.isin({1899, 1900})
        new_dates.loc[bad] = pd.NaT
        queue.loc[:, date_col] = new_dates
    return
