"""A Command line interface for the down ballot project."""

import logging

import click
import coloredlogs

import dbcp
from dbcp.commands.archive import run_archivers
from dbcp.commands.publish import publish_outputs
from dbcp.commands.settings import save_settings
from dbcp.transform.fips_tables import SPATIAL_CACHE
from dbcp.transform.helpers import GEOCODER_CACHE

logger = logging.getLogger(__name__)


@click.group(name="dbcp")
@click.option(
    "--loglevel",
    help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
    default="INFO",
)
def cli(loglevel):
    """A command line interface for the down ballot project."""
    dbcp_logger = logging.getLogger()
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level=loglevel, logger=dbcp_logger)


@cli.command()
@click.option(
    "-dm",
    "--data-mart",
    help="Load the data mart tables to the database",
    default=False,
    is_flag=True,
)
@click.option(
    "-dw",
    "--data-warehouse",
    help="Load the data warehouse tables to the database",
    default=True,
    is_flag=True,
)
@click.option(
    "-clr",
    "--clear-cache",
    help="Delete saved geocoder and spatial join results, forcing fresh API calls and computation.",
    default=False,
    is_flag=True,
)
def etl(data_mart, data_warehouse, clear_cache):
    """Run the ETL process to produce the data warehouse and mart."""
    if clear_cache:
        GEOCODER_CACHE.clear()
        SPATIAL_CACHE.clear()

    if data_warehouse:
        dbcp.etl.etl()
    if data_mart:
        dbcp.data_mart.create_data_marts()


cli.add_command(publish_outputs)
cli.add_command(run_archivers)
cli.add_command(save_settings)

if __name__ == "__main__":
    cli()
