"""Command to run raw data archivers."""
import importlib
import logging
import pkgutil

import click

from dbcp.archivers.utils import AbstractArchiver

logger = logging.getLogger(__name__)


def get_archivers() -> dict[str, AbstractArchiver]:
    """Dynamically import all archivers from the archivers subpackage."""
    archiver_classes = {}

    # Package name and subpackage name
    package_name = "dbcp.archivers"

    # Get package path dynamically
    package_path = importlib.import_module(package_name).__path__

    # Iterate over modules in the subpackage
    for _, module_name, _ in pkgutil.walk_packages(
        path=package_path, prefix=package_name + "."
    ):
        # Import the module dynamically
        _ = importlib.import_module(module_name)

        for archiver in AbstractArchiver.__subclasses__():
            archiver_classes[archiver.archive_name] = archiver
    return archiver_classes


@click.command()
@click.option(
    "--archiver-names",
    "-a",
    multiple=True,
    help="Names of the archivers to run.",
    type=click.Choice(list(get_archivers().keys())),
)
def run_archivers(archiver_names: list[str]):
    """Run the specified archivers."""
    archivers = get_archivers()
    if not archiver_names:
        logger.info("No archiver names provided. Running all archivers.")
        archiver_names = archivers.keys()

    for name, archiver in archivers.items():
        if name in archiver_names:
            logger.info(f"Running archiver {name}.")
            archiver().archive()
        else:
            raise ValueError(f"Archiver {name} not found.")
