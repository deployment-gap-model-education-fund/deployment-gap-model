"""Archiver for Airtable bases."""

import json
import logging
import os

from pyairtable import Api
from pydantic import BaseModel, Field

from dbcp.archivers.utils import AbstractArchiver

logger = logging.getLogger(__name__)


class AirtableBaseInfo(BaseModel):
    """Information about an Airtable base."""

    base_id: str = Field(min_length=17, max_length=17)
    base_name: str


bases = (
    AirtableBaseInfo(
        base_id="appZHPwbPSqIMgphw", base_name="Offshore Wind Locations Synapse Version"
    ),
)


class AirtableArchiver(AbstractArchiver):
    """Archiver for Airtable bases."""

    folder_name = "airtable"

    def __init__(self, api: Api = None):
        """
        Initialize the Airtable archiver.

        Args:
            api: The Airtable API object to use. If None, the API key is read from the AIRTABLE_API_KEY environment variable.
        """
        super().__init__()
        if api is None:
            api_key = os.getenv("AIRTABLE_API_KEY")
            api = Api(api_key)
        self.api = api

    def archive_base(self, base_info: AirtableBaseInfo) -> None:
        """
        Archive a single Airtable base to GCS.

        This method archives the schema of the base which includes the schema of all tables in the base.
        Then it archives the data of each table in the base. The GCS generation number of the
        schema file is saved as metadata in the data files so that the data files can be linked to the schema file.

        Args:
            base_info: Information about the Airtable base to archive.
        """
        logger.info(f"Archiving base {base_info.base_name}")
        base = self.api.base(base_info.base_id)

        # Construct the destination blob name
        base_path = f"{self.folder_name}/{base_info.base_name}"
        destination_blob_name = f"{base_path}/schema.json"

        # Create a blob object in the bucket
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_string(base.schema().json())

        # get the generation number
        schema_generation_number = blob.generation

        # for each table in the base, save the table json to a file in the bucket, add the required metadata
        for table in base.tables():
            logger.info(f"Archiving table {table.name}")
            # Construct the destination blob name
            destination_blob_name = f"{base_path}/{table.name}.json"

            # Create a blob object in the bucket
            blob = self.bucket.blob(destination_blob_name)
            # save the metadata to a file in the bucket
            blob.metadata = {
                "schema_generation_number": schema_generation_number,
                "table_id": table.id,
            }
            blob.upload_from_string(
                json.dumps(
                    table.all(
                        cell_format="string",
                        user_locale="en-us",
                        time_zone="utc",
                    )
                )
            )

    def archive(self):
        """Archive raw tables for a list of Airtable bases to GCS."""
        for base in bases:
            self.archive_base(base)


if __name__ == "__main__":
    AirtableArchiver().archive()
