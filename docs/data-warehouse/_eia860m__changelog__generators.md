# _eia860m__changelog__generators

This table contains change-log style EIA 860M generator data sourced from Catalyst Cooperative's PUDL resources.
This table is not intended for analysis and is largely just a building block for other EIA data warehouse tables (indicated by the leading underscore in the name.) Derived from [this PUDL table](https://docs.catalyst.coop/pudl/en/latest/data_dictionaries/pudl_db.html#core-eia860m-changelog-generators), this table is a changelog of monthly reported generator data. There is a record corresponding to the first instance of a generator and associated characteristics with a report_date column and a valid_until_date column. Whenever any of the reported EIA-860M data was changed for a record, there will be a new changelog record with a new report_date.

## Table Details

**Source:** PUDL / EIA 860M

**Grain:** One row per generator snapshot change

**Primary key column(s):** `plant_id_eia`, `generator_id`, `report_date`

**Purpose:** Historical generator changes across report dates, with validity dates and standardized status fields.

**Additional Details:** EIA-860M includes generator tables with the most up-to-date catalog of EIA generators and their operational status and other generator characteristics. EIA-860M is reported monthly, although for the vast majority of the generators nothing changes month-to-month.

## Transformations

The ETL converts date-like columns to datetimes, renames selected raw fields, and adds state/county FIPS IDs from the raw state and county values.

It preserves the original EIA operational status code in `raw_operational_status_code`, maps the status code to Madrone's numeric status scale in `operational_status_code`, and imputes missing ISO/balancing-authority values for some records when the latest county-level data has exactly one non-null balancing authority code.

## Related Tables

- `eia860m__annual__generators`
- `eia860m__operational_status_codes`
