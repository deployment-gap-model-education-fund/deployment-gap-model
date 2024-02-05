"""Load manually maintained ordinances from BigQuery."""
import google.auth
import pandas as pd
from pandas_gbq import read_gbq


def extract() -> dict[str, pd.DataFrame]:
    """Extract manually maintained ordinances from BigQuery.

    Returns:
        dfs: dictionary of dataframe name to raw dataframe.
    """
    dfs = {}
    credentials, project_id = google.auth.default()
    dfs["manual_ordinances"] = read_gbq(
        """SELECT
    county_id_fips,
    CASE
        WHEN ordinance_status IS NULL THEN NULL
        WHEN ordinance_status IN ('Ordinance/moratorium change in process', 'Prohibitive ordinance/moratorium') THEN TRUE
        ELSE FALSE
    END as ordinance_via_self_maintained
    FROM `local-jobs-econ-dev-fund.airtable_data.county_permitting_info`
    """,
        project_id=project_id,
        credentials=credentials,
    )
    return dfs


if __name__ == "__main__":
    # debugging entry point
    df = extract()["manual_ordinances"]
    print(df.info())
