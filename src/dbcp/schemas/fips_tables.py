"""A module of pandera schemas for the FIPS data source."""
import pandas as pd
import pandera as pa

TABLE_SCHEMAS = {
    "county_fips": pa.DataFrameSchema(
        {
            "state_id_fips": pa.Column(pd.StringDtype, nullable=False),
            "county_id_fips": pa.Column(pd.StringDtype, nullable=False),
            "county_name": pa.Column(pd.StringDtype, nullable=False),
        },
        strict=True,
        coerce=True,
    ),
    "state_fips": pa.DataFrameSchema(
        {
            "state_id_fips": pa.Column(pd.StringDtype, nullable=False),
            "state_name": pa.Column(pd.StringDtype, nullable=False),
            "state_abbrev": pa.Column(pd.StringDtype, nullable=False),
        },
        strict=True,
        coerce=True,
    ),
}
