"""Save settings to a YAML file."""
import click

from dbcp.archivers.utils import ExtractionSettings


@click.command()
def save_settings():
    """Update and save the ETL settings to a YAML file."""
    es = ExtractionSettings.from_yaml("/app/dbcp/etl_settings.yaml")
    es.update_archive_generation_numbers()
    es.to_yaml("/app/dbcp/settings.yaml")
