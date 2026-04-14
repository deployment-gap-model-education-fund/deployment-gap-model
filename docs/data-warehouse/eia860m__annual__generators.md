# eia860m__annual__generators

This table contains the most recent annual EIA 860M generator records sourced from Catalyst Cooperative's PUDL resources. It contains generator data from the most recent complete calendar year and is all "final release" data (not updated monthly by EIA 860M release). This table is used for exploring the most recent yearly attributes about generators, rather than viewing a change log or monthly updates to the generators.

## Table Details

**Source:** PUDL / EIA 860M

**Grain:** One row per generator per annual report date

**Primary key column(s):** `plant_id_eia`, `generator_id`, `report_date`

**Purpose:** Latest annual generator attributes with county/state FIPS added.

## Related Tables

- `_eia860m__changelog__generators`
- `eia860m__operational_status_codes`
- `census__county_fips`
- `census__state_fips`
