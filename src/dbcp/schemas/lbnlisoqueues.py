"""A module of pandera schemas for the LBNL ISO Queue data source."""
import pandas as pd
import pandera as pa

from dbcp.schemas import CoercedInt64

TABLE_SCHEMAS = {
    "iso_projects": pa.DataFrameSchema(
        {
            "project_id": pa.Column(pd.Int64Dtype),
            "date_proposed_raw": pa.Column(pd.StringDtype, nullable=True),
            "developer": pa.Column(pd.StringDtype, nullable=True),
            "entity": pa.Column(pd.StringDtype),
            "interconnection_status_lbnl": pa.Column(pd.StringDtype, nullable=True),
            "interconnection_status_raw": pa.Column(pd.StringDtype, nullable=True),
            "point_of_interconnection": pa.Column(pd.StringDtype, nullable=True),
            "project_name": pa.Column(pd.StringDtype, nullable=True),
            "queue_date": pa.Column(pa.DateTime, nullable=True),
            "queue_id": pa.Column(pd.StringDtype, nullable=True),
            "queue_status": pa.Column(pd.StringDtype),
            "queue_year": pa.Column(CoercedInt64(), nullable=True),
            "region": pa.Column(pd.StringDtype, nullable=True),
            "resource_type_lbnl": pa.Column(pd.StringDtype, nullable=True),
            "utility": pa.Column(pd.StringDtype, nullable=True),
            "year_proposed": pa.Column(CoercedInt64(), nullable=True),
            "date_proposed": pa.Column(pa.DateTime, nullable=True),
            "date_operational": pa.Column(pa.DateTime, nullable=True),
            "days_in_queue": pa.Column(CoercedInt64(), nullable=True),
            "queue_date_raw": pa.Column(pd.StringDtype, nullable=True),
            "year_operational": pa.Column(CoercedInt64(), nullable=True),
            "date_withdrawn_raw": pa.Column(pd.StringDtype, nullable=True),
            "withdrawl_reason": pa.Column(pd.StringDtype, nullable=True),
            "year_withdrawn": pa.Column(CoercedInt64(), nullable=True),
            "date_withdrawn": pa.Column(pa.DateTime, nullable=True),
        },
        strict=True,
        coerce=True,
    ),
    "iso_locations": pa.DataFrameSchema(
        {
            "project_id": pa.Column(pd.Int64Dtype),
            "raw_county_name": pa.Column(pd.StringDtype, nullable=True),
            "raw_state_name": pa.Column(pd.StringDtype, nullable=True),
            "state_id_fips": pa.Column(pd.StringDtype, nullable=True),
            "county_id_fips": pa.Column(pd.StringDtype, nullable=True),
            "geocoded_locality_name": pa.Column(pd.StringDtype, nullable=True),
            "geocoded_locality_type": pa.Column(pd.StringDtype, nullable=True),
            "geocoded_containing_county": pa.Column(pd.StringDtype, nullable=True),
        },
        strict=True,
        coerce=True,
    ),
    "iso_resource_capacity": pa.DataFrameSchema(
        {
            "project_id": pa.Column(pd.Int64Dtype),
            "resource": pa.Column(pd.StringDtype, nullable=True),
            "resource_clean": pa.Column(pd.StringDtype, nullable=True),
            "resource_class": pa.Column(pd.StringDtype, nullable=True),
            "project_class": pa.Column(pd.StringDtype, nullable=True),
            "capacity_mw": pa.Column(float, nullable=True),
        },
        strict=True,
        coerce=True,
    ),
}

ISO_FOR_TABLEAU = (
    TABLE_SCHEMAS["iso_projects"]
    .add_columns(TABLE_SCHEMAS["iso_locations"].columns)
    .add_columns(TABLE_SCHEMAS["iso_resource_capacity"].columns)
    .add_columns(
        pa.DataFrameSchema(
            {
                "co2e_tpy": pa.Column(float, nullable=True),
                "index": pa.Column(pd.Int64Dtype),
            }
        ).columns
    )
    .remove_columns(
        [
            "geocoded_locality_name",
            "geocoded_locality_type",
            "geocoded_containing_county",
        ]
    )
)

TABLE_SCHEMAS["iso_for_tableau"] = ISO_FOR_TABLEAU
