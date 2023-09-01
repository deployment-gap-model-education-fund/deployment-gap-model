"""Module for cleaning Ballot Ready data."""
import pandas as pd

from pudl.helpers import add_fips_ids

DATETIME_COLUMNS = ["race_created_at", "race_updated_at", "election_day"]


def _normalize_entities(ballot_ready: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Normalize ballot ready data into elections, position and race entities.

    Args:
        ballot_ready: Exploded, lightly cleaned raw data.

    Returns:
        trns_dfs: dataframes for elecitons, positions and races.
    """
    trns_dfs = {}
    # Elections
    election_fields = [
        "election_id",
        "election_name",
        "election_day",
    ]
    assert (
        (ballot_ready.groupby("election_id")[election_fields].nunique() <= 1)
        .all()
        .all()
    ), "There is duplicate entity information in the elections dataframe."

    br_elections = ballot_ready.drop_duplicates(subset=election_fields)[
        election_fields
    ].copy()
    assert br_elections.election_id.is_unique, "election_id is not unique."

    trns_dfs["br_elections"] = br_elections

    # Positions
    position_fields = [
        "reference_year",
        "position_id",
        "position_name",
        "sub_area_name",
        "sub_area_value",
        "sub_area_name_secondary",
        "sub_area_value_secondary",
        "raw_state",
        "level",
        "tier",
        "is_judicial",
        "is_retention",
        "normalized_position_id",
        "normalized_position_name",
        "frequency",
        "partisan_type",
    ]
    # position_id == 156594 is the only position with two freqquencies and reference_years
    # Create a new index for it
    new_index = ballot_ready.position_id.max() + 1
    assert new_index not in ballot_ready.position_id
    ballot_ready.loc[ballot_ready.race_id == 2020783, "position_id"] = new_index
    assert (
        (ballot_ready.groupby("position_id")[position_fields].nunique() <= 1)
        .all()
        .all()
    ), "There is duplicate entity information in the positions dataframe."
    br_positions = ballot_ready.drop_duplicates(subset=position_fields)[
        position_fields
    ].copy()
    trns_dfs["br_positions"] = br_positions

    # Races
    race_fields = [
        "race_id",
        "is_primary",
        "is_runoff",
        "is_unexpired",
        "number_of_seats",
        "race_created_at",
        "race_updated_at",
    ]
    assert (
        (ballot_ready.groupby("race_id")[race_fields].nunique() <= 1).all().all()
    ), "There is duplicate entity informaiton in the races table."
    # Add some one to many fields to the races table dataframe.
    race_fields += [
        "raw_state",
        "raw_county",
        "state_id_fips",
        "county_id_fips",
        "election_id",
        "position_id",
    ]
    br_races = ballot_ready.drop_duplicates(subset=race_fields)[race_fields].copy()
    assert len(br_races) == len(ballot_ready)

    trns_dfs["br_races"] = br_races
    return trns_dfs


def _explode_counties(raw_ballot_ready: pd.DataFrame) -> pd.DataFrame:
    """Correct datatypes and explode counties columns.

    Args:
        raw_ballot_ready: raw ballot ready data.
    Returns:
        ballot_ready: lightly cleaned and exploded dataframe.
    """
    # Correct datatypes
    ballot_ready = raw_ballot_ready.convert_dtypes()
    for col in DATETIME_COLUMNS:
        ballot_ready[col] = pd.to_datetime(ballot_ready[col])

    # Explode counties column
    ballot_ready["counties"] = (
        ballot_ready.counties.str.replace('"', "").str[1:-1].str.split(", ")
    )

    exp_ballot_ready = ballot_ready.explode("counties").rename(
        columns={"counties": "county"}
    )

    duplicate_race = exp_ballot_ready.duplicated(
        subset=["county", "race_id"], keep=False
    )
    # Initial batch of raw data has duplicates in counties
    assert (
        duplicate_race.sum() <= 506
    ), "Found more duplicate county/race combinations that expected."

    # Drop duplicates. A later version of ballot ready data will remedy this problem.
    ballot_ready = exp_ballot_ready.drop_duplicates(subset=["county", "race_id"])
    assert ~ballot_ready.duplicated(subset=["county", "race_id"], keep=False).any()

    # Add state and county fips codes
    # Fix LaSalle Parish spelling to match addfips library
    ballot_ready.loc[
        (ballot_ready.county == "LaSalle Parish"), "county"
    ] = "La Salle Parish"
    ballot_ready = add_fips_ids(ballot_ready)

    # Valdez-Cordova Census Area was split into two areas in 2019
    # https://www.census.gov/programs-surveys/geography/technical-documentation/county-changes/2010.html
    # All elections are state and federal level so I will duplicate the races for the two new census areas
    valdez = ballot_ready.query("county_id_fips == '02261'")
    assert valdez.level.isin(
        ["state", "federal"]
    ).all(), "Found a local election in the Valdez-Cordova Census Area!"

    ballot_ready = ballot_ready[ballot_ready.county_id_fips != "02261"].copy()

    valdez_corrections = [
        {"county": "Chugach Census Area", "county_id_fips": "02063"},
        {"county": "Copper River Census Area", "county_id_fips": "02066"},
    ]

    valdez_corrections_dfs = []
    for cor in valdez_corrections:
        corrected_df = valdez.copy()
        for field, value in cor.items():
            corrected_df[field] = value
        valdez_corrections_dfs.append(corrected_df)

    ballot_ready = pd.concat(valdez_corrections_dfs + [ballot_ready])

    # Drop unused columns
    ballot_ready = ballot_ready.drop(columns=["position_description", "id"])
    ballot_ready = ballot_ready.rename(
        columns={"county": "raw_county", "state": "raw_state"}
    )
    return ballot_ready


def transform(raw_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Clean the Ballot Ready data.

    Transformations include:
    * Correct datatypes
    * Explode counties columns

    Args:
        raw_dfs: dictionary of dataframe names to raw dataframes.

    Returns
        trns_dfs: dictionary of dataframe names to cleaned dataframes.
    """
    raw_ballot_ready = raw_dfs["raw_ballot_ready"]
    ballot_ready = _explode_counties(raw_ballot_ready)
    return _normalize_entities(ballot_ready)
