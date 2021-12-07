"""A Command line interface for the down ballot project."""
import argparse
import logging
import sys

import coloredlogs

import dbcp


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
    arguments = parser.parse_args()
    return arguments


def main():
    """Parse command line and initialize PUDL DB."""
    # Display logged output from the PUDL package:
    dbcp_logger = logging.getLogger()
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=dbcp_logger)

    args = parse_command_line()

    dbcp.etl.etl(args)


if __name__ == "__main__":
    sys.exit(main())
