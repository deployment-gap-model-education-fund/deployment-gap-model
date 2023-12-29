"""
Extract gridstatus iso queues data from private bucket archive.

gridstatus code points directly at interconnection queue spreadsheets
on ISO queues websites. These spreadsheets can change without notice
and break the gridstatus API. We have a private archive of the gridstatus data
that allows us to pin the ETL code to a specific version of the raw
data. The version numbers are automatically generated by Google Cloud Storage
Object Versioning.
"""
import logging

import pandas as pd

import dbcp

logger = logging.getLogger(__name__)

# These are the latest revision numbers as of 12/04/23
ISO_QUEUE_VERSIONS: dict[str, str] = {
    "miso": "1701730379212665",
    "caiso": "1701730379782773",
    "pjm": "1701730380346804",
    "ercot": "1701730380870486",
    "spp": "1701730381410448",
    "nyiso": "1701730381901584",
    "isone": "1701730382409516",
}


def extract(iso_queue_versions: dict[str, str] = ISO_QUEUE_VERSIONS):
    """Extract gridstatus ISO Queue data."""
    iso_queues: dict[str, pd.DataFrame] = {}
    credentials = dbcp.extract.helpers.get_gcp_credentials()
    for iso, revision_num in iso_queue_versions.items():
        uri = f"gs://gridstatus-archive/interconnection_queues/{iso}.parquet"
        path = dbcp.extract.helpers.cache_gcs_archive_file_locally(
            uri=uri, revision_num=revision_num, credentials=credentials
        )

        iso_queues[iso] = pd.read_parquet(path)

    return iso_queues