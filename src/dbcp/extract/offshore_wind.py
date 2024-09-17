"""Extract offshore wind location and project data from CSVs extracted from Airtable."""

import pandas as pd

import dbcp


def extract(*, locations_uri: str, projects_uri: str) -> dict[str, pd.DataFrame]:
    """Extract offshore wind location and project data.

    Args:
        locations_uri: path to CSV file
        projects_uri: path to CSV file

    Returns:
        dict[str, pd.DataFrame]: raw data, with keys "offshore_locations" and "offshore_projects"
    """
    offshore_transformed_dfs = {}

    # TODO: Devise a better way to grab the generation number.
    # I didn't want to change it up too much because I don't want to deal with
    # changing the gridstatus extraction code. I could imagine just passing
    # an ArchiveData or Blob object.
    projects_uri, projects_generation_num = str(projects_uri).split("#")
    locations_uri, locations_generation_num = str(locations_uri).split("#")

    projects_path = dbcp.extract.helpers.cache_gcs_archive_file_locally(
        projects_uri, generation_num=projects_generation_num
    )
    locations_path = dbcp.extract.helpers.cache_gcs_archive_file_locally(
        locations_uri, generation_num=locations_generation_num
    )

    offshore_transformed_dfs["raw_offshore_wind_projects"] = pd.read_csv(projects_path)
    offshore_transformed_dfs["raw_offshore_wind_locations"] = pd.read_csv(
        locations_path
    )

    return offshore_transformed_dfs
