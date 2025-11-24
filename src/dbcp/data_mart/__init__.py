"""Modules to create tables in the 'data mart' for direct use by users."""

import importlib
import logging
import pkgutil

logger = logging.getLogger(__name__)


def get_data_mart_modules():
    """Get the modules in the data mart directory in prep for creating data mart tables."""
    modules_to_skip = {
        "helpers",  # helper code; no tables
        "co2_dashboard",  # obsolete but code imported elsewhere
    }
    modules = []
    for module_info in pkgutil.iter_modules(__path__):
        if module_info.name in modules_to_skip:
            continue
        module = importlib.import_module(f"{__name__}.{module_info.name}")
        modules.append((module, module_info))
    return modules
