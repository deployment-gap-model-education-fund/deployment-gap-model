"""A Command line interface for the down ballot project."""
import argparse
import logging
import sys

import coloredlogs

import dbcp
from dbcp.transform.helpers import GEOCODER_CACHE

def parse_command_line():
    """
    Parse script command line arguments. See the -h option.

    Returns:
        dict: A dictionary mapping command line arguments to their values.

    """
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        '-c',
        '--csv',
        action='store_true',
        default=False,
        help="Write tables to csvs. This will overwrite existing csvs.",
    )
    parser.add_argument(
        '-bq',
        '--upload-to-bigquery',
        action='store_true',
        default=False,
        help="Loads tables to BigQuery.",
    )
    parser.add_argument(
        "--loglevel",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
        default="INFO",
    )
    parser.add_argument(
        '-clr'
        '--clear-geocoder-cache',
        action='store_true',
        default=False,
        help="Delete saved geocoder results, forcing fresh API calls.",
    )
    arguments = parser.parse_args()
    return arguments


def main():
    """Parse command line and initialize PUDL DB."""
    args = parse_command_line()

    # Display logged output from the PUDL package:
    dbcp_logger = logging.getLogger()
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level=args.loglevel, logger=dbcp_logger)

    if args.clr or args.bq:
        GEOCODER_CACHE.clear()
    dbcp.etl.etl(args)


if __name__ == "__main__":
    sys.exit(main())
