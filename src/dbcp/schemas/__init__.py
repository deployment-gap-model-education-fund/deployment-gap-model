"""A subpackage to define and organize DBCP database tables by data source."""

import importlib
import pkgutil

import pandas as pd
from pandera import dtypes
from pandera.engines import pandas_engine


@pandas_engine.Engine.register_dtype
@dtypes.immutable
class CoercedInt64(pandas_engine.INT64):
    """Pandera datatype that coerces pd.Int64 data type errors."""

    def coerce(self, series: pd.Series) -> pd.Series:
        """Coerce a pandas.Series to date types."""
        series = pd.to_numeric(series, errors="coerce")
        return series.astype("Int64")


TABLE_SCHEMAS = {}
for module_info in pkgutil.iter_modules(__path__):
    module = importlib.import_module(f"{__name__}.{module_info.name}")
    try:
        schemas = module.TABLE_SCHEMAS
    except AttributeError:
        raise AttributeError(
            f"{module_info.name} has no attribute 'TABLE_SCHEMAS'."
            "Make sure the schema module contains a TABLE_SCHEMAS dictionary."
        )
    TABLE_SCHEMAS.update(schemas)
