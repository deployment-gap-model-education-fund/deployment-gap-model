"""A Command line interface for the down ballot project."""
import logging
import sys

import coloredlogs

import dbcp


def main():
    """Parse command line and initialize PUDL DB."""
    # Display logged output from the PUDL package:
    dbcp_logger = logging.getLogger()
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=dbcp_logger)

    dbcp.etl.etl()


if __name__ == "__main__":
    sys.exit(main())
