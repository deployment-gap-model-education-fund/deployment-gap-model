# columbia_reldi_local_opposition__local_ordinance

This table contains local ordinance records from Columbia's Renewable Energy Legal Defense Initiative (RELDI) opposition-to-renewables source.

## Table Details

**Source:** Columbia RELDI local opposition

**Grain:** One row per local ordinance entry

**Primary key column(s):** `county_id_fips`, `state_id_fips`

**Purpose:** Local ordinance text, energy type, year fields, and county/state enrichment.

## Transformations

The ETL strips whitespace from text fields, applies a small set of manual locality name corrections, removes locality prefixes that make geocoding less reliable, and adds county/state FIPS codes with backup geocoding.

It also extracts year-like values from the ordinance text and summarizes them into `earliest_year_mentioned`, `latest_year_mentioned`, and `n_years_mentioned`. These fields are helpful context, but they should not always be interpreted as enacted-year fields.

## Related Tables

- `columbia_reldi_local_opposition__state_policy`
- `census__county_fips`
- `census__state_fips`
