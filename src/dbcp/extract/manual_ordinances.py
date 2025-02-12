"""Load manually maintained ordinances from BigQuery."""

import pandas as pd

from dbcp.extract.helpers import cache_gcs_archive_file_locally, extract_airtable_data


def extract(counties_uri: str) -> dict[str, pd.DataFrame]:
    """Extract manually maintained ordinances from BigQuery.

    Returns:
        dfs: dictionary of dataframe name to raw dataframe.
    """
    dfs = {}
    counties_uri, counties_generation_num = str(counties_uri).split("#")

    counties_path = cache_gcs_archive_file_locally(
        counties_uri, generation_num=counties_generation_num
    )

    manual_ordinances_counties = extract_airtable_data(counties_path)
    # drop all rows that are missing all data
    manual_ordinances_counties = manual_ordinances_counties.dropna(how="all")

    manual_ordinances_counties["Ordinance Status"] = manual_ordinances_counties[
        "Ordinance Status"
    ].isin(
        ("Ordinance/moratorium change in process", "Prohibitive ordinance/moratorium")
    )
    manual_ordinances_counties = manual_ordinances_counties.rename(
        columns={
            "FIPS": "county_id_fips",
            "Ordinance Status": "ordinance_via_self_maintained",
        }
    )

    manual_ordinances_counties = manual_ordinances_counties[
        ["county_id_fips", "ordinance_via_self_maintained"]
    ].copy()

    dfs["manual_ordinances"] = manual_ordinances_counties
    return dfs


if __name__ == "__main__":
    # debugging entry point
    df = extract()["manual_ordinances"]
    print(df.info())
