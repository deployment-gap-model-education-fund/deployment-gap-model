"""Extract LJEDF county demographics and election results from GCS archive."""

import pandas as pd

from dbcp.constants import DATA_DIR
from dbcp.extract.helpers import cache_gcs_archive_file_locally, load_yml_file


def extract() -> dict[str, pd.DataFrame]:
    """Extract archived LJEDF county demographics and election results."""
    file_paths = load_yml_file(DATA_DIR / "file_paths.yml")
    return {
        "raw_civis_demographics": pd.read_csv(
            cache_gcs_archive_file_locally(file_paths["civis_demographics"].item())
        ),
        "raw_civis_2024_president_results_county": pd.read_csv(
            cache_gcs_archive_file_locally(
                file_paths["civis_president_results_2024"].item()
            )
        ),
    }
