"""Utility functions and classes for archivers."""
from abc import ABC, abstractmethod

import google.auth
from google.cloud import storage


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
