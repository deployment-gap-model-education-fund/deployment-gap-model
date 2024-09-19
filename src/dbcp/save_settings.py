"""Save settings to a YAML file."""
from dbcp.archivers.utils import ExtractionSettings


def save_settings():
    """Save settings to a YAML file."""
    es = ExtractionSettings.from_yaml("/app/dbcp/settings.yaml")
    # es = ExtractionSettings.from_archive_names([projects_uri, locations_uri])
    es.update_archive_generation_numbers()
    es.to_yaml("/app/dbcp/settings.yaml")


if __name__ == "__main__":
    save_settings()
