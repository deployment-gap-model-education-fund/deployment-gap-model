"""Utility functions and classes for archivers."""

from abc import ABC, abstractmethod

import google.auth
import yaml
from google.cloud import storage
from pydantic import BaseModel


class AbstractArchiver(ABC):
    """Abstract class for archiving data."""

    bucket_name: str = "dgm-archive"
    folder_name: str

    def __init__(self):
        """Initialize the archiver."""
        credentials, project_id = google.auth.default()
        self.client = storage.Client(credentials=credentials, project=project_id)
        self.bucket = self.client.get_bucket(self.bucket_name)

    @abstractmethod
    def archive(self):
        """Archive raw data to GCS."""
        raise NotImplementedError()


class ArchivedData(BaseModel):
    """Model with information about a single archived object."""

    name: str
    generation_num: int | None
    pinned: bool = False
    metadata: dict[str, str] = {}
    # Maybe include a date field for when the data was archived

    def get_full_path(self) -> str:
        """Get the full path of the archived data."""
        return f"{self.name}#{self.generation_num}"


class ExtractionSettings:
    """Settings for extracting data from a source."""

    def __init__(
        self, archived_data: dict[str, ArchivedData], bucket_name: str = "dgm-archive"
    ):
        """Initialize the ExtractionSettings object."""
        credentials, project_id = google.auth.default()
        self.client = storage.Client(credentials=credentials, project=project_id)
        self.bucket = self.client.get_bucket(bucket_name)

        self.archived_data = archived_data

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "ExtractionSettings":
        """Create an ExtractionSettings object from a YAML file."""
        with open(yaml_path, "r") as file:
            archive_dicts = yaml.safe_load(file)
        archived_data = {
            archive_dict["name"]: ArchivedData(**archive_dict)
            for archive_dict in archive_dicts
        }
        return cls(archived_data=archived_data)

    def to_yaml(self, yaml_path: str):
        """Save the ExtractionSettings object to a YAML file."""
        archive_dicts = [archive.dict() for archive in self.archived_data.values()]
        with open(yaml_path, "w") as file:
            yaml.dump(archive_dicts, file, default_flow_style=False)

    @classmethod
    def from_archive_names(
        cls, archive_names: list[str], bucket_name: str = "dgm-archive"
    ) -> "ExtractionSettings":
        """Create an ExtractionSettings object from a list of archive names."""
        archive_data = {
            archive_name: ArchivedData(name=archive_name)
            for archive_name in archive_names
        }
        return cls(archived_data=archive_data, bucket_name=bucket_name)

    def get_full_archive_uri(self, archive_name: str) -> str:
        """Get the full archive name with the folder name."""
        try:
            archive = self.archived_data[archive_name]
        except KeyError:
            raise KeyError(f"Archive {archive_name} not found in the settings.")
        return f"gs://{self.bucket.name}/{archive.get_full_path()}"

    def update_archive_generation_numbers(self):
        """Update the generation numbers for the archived data."""
        for archive in self.archived_data.values():
            if not archive.pinned or archive.generation_num is None:
                blob = self.bucket.get_blob(archive.name)
                if blob is None:
                    raise ValueError(
                        f"Blob {archive.name} does not exist in the {self.bucket.name} bucket"
                    )
                archive.generation_num = blob.generation
                archive.metadata = blob.metadata
            assert (
                archive.generation_num is not None
            ), f"Generation number for {archive.name} is None"
