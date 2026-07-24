"""Extract LJEDF county demographics and election results from GCS archive."""

import pandas as pd

from dbcp.extract.helpers import cache_gcs_archive_file_locally

CIVIS_DEMOGRAPHICS_URI = "gs://dgm-archive/ljedf/civis_demographics.csv"
PRESIDENT_2024_RESULTS_URI = "gs://dgm-archive/ljedf/2024_president_results_county.csv"


def extract() -> dict[str, pd.DataFrame]:
    """Extract archived LJEDF county demographics and election results."""
    return {
        "raw_ljedf_civis_demographics": pd.read_csv(
            cache_gcs_archive_file_locally(CIVIS_DEMOGRAPHICS_URI)
        ),
        "raw_ljedf_2024_president_results_county": pd.read_csv(
            cache_gcs_archive_file_locally(PRESIDENT_2024_RESULTS_URI)
        ),
    }
