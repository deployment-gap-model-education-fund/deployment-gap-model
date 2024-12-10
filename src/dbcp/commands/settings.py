"""Save settings to a YAML file."""

import logging

import click

from dbcp.archivers.utils import ExtractionSettings

logger = logging.getLogger(__name__)


@click.command()
def save_settings():
    """Update and save the ETL settings to a YAML file."""
    logger.info("Saving settings to settings.yaml")
    es = ExtractionSettings.from_yaml("/app/dbcp/settings.yaml")
    es.update_archive_generation_numbers()
    es.to_yaml("/app/dbcp/settings.yaml")
