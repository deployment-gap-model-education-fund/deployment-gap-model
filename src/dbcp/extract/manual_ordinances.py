"""Load manually maintained ordinances from BigQuery."""
import pandas as pd
from pandas_gbq import read_gbq

from dbcp.helpers import GCP_PROJECT_ID, _get_bigquery_credentials


def extract() -> dict[str, pd.DataFrame]:
    """Extract manually maintained ordinances from BigQuery.

    Returns:
        dfs: dictionary of dataframe name to raw dataframe.
    """
    dfs = {}
    credentials = _get_bigquery_credentials()
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
        project_id=GCP_PROJECT_ID,
        credentials=credentials,
    )
    return dfs


if __name__ == "__main__":
    # debugging entry point
    df = extract()["manual_ordinances"]
    print(df.info())
