"""A Command line interface for the down ballot project."""
import argparse
import logging
import sys

import coloredlogs

import dbcp
from dbcp.transform.fips_tables import SPATIAL_CACHE
from dbcp.transform.helpers import GEOCODER_CACHE

logger = logging.getLogger(__name__)


def parse_command_line():
    """
    Parse script command line arguments. See the -h option.

    Returns:
        dict: A dictionary mapping command line arguments to their values.

    """
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "-dm",
        "--data-mart",
        action="store_true",
        default=False,
        help="Build the data marts.",
    )
    parser.add_argument(
        "-e",
        "--etl",
        action="store_true",
        default=False,
        help="Run the etl to produce the data warehouse.",
    )
    parser.add_argument(
        "--loglevel",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
        default="INFO",
    )
    parser.add_argument(
        "-clr",
        "--clear-cache",
        action="store_true",
        default=False,
        help="Delete saved geocoder and spatial join results, forcing fresh API calls and computation.",
    )
    arguments = parser.parse_args()
    return arguments


def main():
    """Parse command line and initialize PUDL DB."""
    args = parse_command_line()

    # Display logged output from the PUDL package:
    dbcp_logger = logging.getLogger()
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level=args.loglevel, logger=dbcp_logger)

    if args.clear_cache:
        GEOCODER_CACHE.clear()
        SPATIAL_CACHE.clear()

    if args.etl:
        dbcp.etl.etl(args)
    if args.data_mart:
        dbcp.data_mart.create_data_marts(args)


if __name__ == "__main__":
    sys.exit(main())
