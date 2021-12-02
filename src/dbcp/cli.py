"""A Command line interface for the down ballot project."""
import sys

import dbcp


def main():
    """Parse command line and initialize PUDL DB."""
    # Display logged output from the PUDL package:
    dbcp.etl.etl()


if __name__ == "__main__":
    sys.exit(main())
