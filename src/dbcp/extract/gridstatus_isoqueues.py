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

ISO_QUEUE_VERSIONS: dict[str, str] = {
    "miso": "1738156310805368",
    "miso-pre-2017": "1709776311574737",
    "caiso": "1738156311098902",
    "pjm": "1738156311428625",
    "ercot": "1738156311725164",
    "spp": "1738156312034206",
    "nyiso": "1738156312328183",
    "isone": "1738156312632000",
}


def extract(iso_queue_versions: dict[str, str] = ISO_QUEUE_VERSIONS):
    """Extract gridstatus ISO Queue data."""
    iso_queues: dict[str, pd.DataFrame] = {}
    for iso, generation_num in iso_queue_versions.items():
        # MISO is an exception to the rule because we need multiple snapshots of the data
        filename = iso if iso != "miso-pre-2017" else "miso"
        uri = f"gs://dgm-archive/gridstatus/interconnection_queues/parquet/{filename}.parquet"
        path = dbcp.extract.helpers.cache_gcs_archive_file_locally(
            uri=uri, generation_num=generation_num
        )

        iso_queues[iso] = pd.read_parquet(path)

    return iso_queues
