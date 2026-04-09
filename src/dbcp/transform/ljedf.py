"""Transform archived LJEDF county demographics and election results."""

import pandas as pd


def _transform_civis_demographics(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Standardize and clean archived Civis demographics and Biden election data."""
    civis = raw_df.rename(
        columns={
            "sii_county": "county_id_fips",
            "female_percent": "female_pct",
            "male_percent": "male_pct",
            "white_percent": "white_pct",
            "black_percent": "black_pct",
            "asian_percent": "asian_pct",
            "american_indian_percent": "american_indian_pct",
            "minority_percent": "minority_pct",
            "age_25_to_64": "age_25_to_64_pct",
            "votes_biden_2020": "biden_2020_pct",
            "below_college": "below_college_pct",
        }
    ).copy()

    civis = civis[civis["county_id_fips"].notna()].copy()
    civis["county_id_fips"] = (
        civis["county_id_fips"].astype("Int64").astype(str).str.zfill(5)
    )
    civis["registered_voters"] = civis["registered_voters"].astype("Int64")

    keep_cols = [
        "county_id_fips",
        "registered_voters",
        "female_pct",
        "male_pct",
        "white_pct",
        "black_pct",
        "asian_pct",
        "american_indian_pct",
        "minority_pct",
        "age_25_to_64_pct",
        "biden_2020_pct",
        "projected_dems_pct",
        "below_college_pct",
    ]
    civis = civis.loc[:, keep_cols].convert_dtypes()
    invalid_projected_dems_pct = ~(
        civis["projected_dems_pct"].between(0, 100) | civis["projected_dems_pct"].isna()
    )
    assert invalid_projected_dems_pct.sum() <= 1, (
        "Found more invalid projected_dems_pct values than expected."
    )
    civis.loc[invalid_projected_dems_pct, "projected_dems_pct"] = pd.NA
    assert civis["county_id_fips"].is_unique, "Civis county FIPS are not unique."
    return civis


def _transform_2024_president_results(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Get the Harris 2024 vote percentage."""
    election = raw_df.copy()
    election["county_id_fips"] = (
        election["FIPS"].astype("Int64").astype(str).str.zfill(5)
    )
    election = election.rename(
        columns={
            "Geographic Name": "geographic_name",
            "Geographic Subtype": "geographic_subtype",
            "Total Vote": "total_votes",
            "Kamala D_ Harris": "harris_votes",
            "Donald J_ Trump": "trump_votes",
            "Robert F_ Kennedy Jr_": "kennedy_votes",
        }
    )
    for col in ["total_votes", "harris_votes", "trump_votes", "kennedy_votes"]:
        election[col] = pd.to_numeric(election[col], errors="coerce")
    election["harris_2024_pct"] = (
        (election["harris_votes"] / election["total_votes"]) * 100
    ).round(2)
    election = election.loc[
        :,
        [
            "county_id_fips",
            "harris_2024_pct",
        ],
    ].convert_dtypes()
    assert election["county_id_fips"].is_unique, (
        "2024 election county FIPS are not unique."
    )
    return election


def transform(
    raw_dfs: dict[str, pd.DataFrame], county_fips: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    """Build a county-level table indexed on county FIPS with election results and demographics.

    Args:
        raw_dfs: dictionary with keys for the raw civis demographics data and the
                 2024 presidential results data.
        county_fips: the census__county_fips table, used as the county FIPS index to
                     construct the table from

    Returns:
        a dictionary with ljedf__private__counties__election_results as key and the
        transformed table containing Biden and Harris percentage of vote capture results
        and demographics data.

    """
    civis = _transform_civis_demographics(raw_dfs["raw_ljedf_civis_demographics"])
    election = _transform_2024_president_results(
        raw_dfs["raw_ljedf_2024_president_results_county"]
    )

    fips_index = county_fips.loc[:, ["county_id_fips", "state_id_fips"]].copy()
    # exclude territories
    fips_index = fips_index[
        pd.to_numeric(fips_index["state_id_fips"], errors="coerce") < 60
    ].copy()

    county_politics = fips_index.merge(
        civis, how="left", on="county_id_fips", validate="1:1"
    )
    county_politics = county_politics.merge(
        election.loc[:, ["county_id_fips", "harris_2024_pct"]],
        how="left",
        on="county_id_fips",
        validate="1:1",
    )
    county_politics = county_politics.drop(columns=["state_id_fips"])
    county_politics = county_politics.sort_values("county_id_fips").reset_index(
        drop=True
    )

    pct_cols = [col for col in county_politics.columns if col.endswith("_pct")]
    for col in pct_cols:
        valid = county_politics[col].between(0, 100) | county_politics[col].isna()
        assert valid.all(), f"Found out-of-range percentage values in {col}."

    assert county_politics["county_id_fips"].is_unique, (
        "Output county FIPS are not unique."
    )
    return {"ljedf__private__counties__election_results": county_politics}
