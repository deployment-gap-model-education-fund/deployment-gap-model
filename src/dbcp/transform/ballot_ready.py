"""Module for cleaning Ballot Ready data."""
import pandas as pd

from pudl.helpers import add_fips_ids

DATETIME_COLUMNS = ["race_created_at", "race_updated_at", "election_day"]


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
    ballot_ready = raw_dfs["raw_ballot_ready"]
    # Correct datatypes
    ballot_ready = ballot_ready.convert_dtypes()
    for col in DATETIME_COLUMNS:
        ballot_ready[col] = pd.to_datetime(ballot_ready[col])

    # Explode counties column
    ballot_ready["counties"] = ballot_ready.counties.str[1:-1].str.split(",")

    exp_ballot_ready = ballot_ready.explode("counties")
    exp_ballot_ready = exp_ballot_ready.rename(columns={"counties": "county"})

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
    ballot_ready = add_fips_ids(ballot_ready)

    trns_dfs = {}
    trns_dfs["ballot_ready_counties"] = ballot_ready
    return trns_dfs
