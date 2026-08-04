# fyi__private__locations

This private warehouse table contains normalized location data for interconnection.fyi queue projects. We use a geocoder to validate county locational information and FIPS codes, then join it onto the Census county FIPS backbone.

## Table Details

**Source:** interconnection.fyi queues

**Grain:** One row per project-location record

**Primary key column(s):** `project_id`

**Purpose:** County/state/location details for projects with location data.

## Transformations

The ETL splits raw project location fields out of `fyi__private__projects` into a normalized locations table and drops records where both raw state and county are missing.

Before geocoding, the transform applies manual county/state fill-ins for known source-data gaps. It then adds `state_id_fips`, `county_id_fips`, `geocoded_locality_name`, `geocoded_locality_type`, and `geocoded_containing_county`, while preserving the original raw county/state names in the final output.

We're currently dropping the FYI provided `fips_codes` column in favor of the geocded value, but as a future improvement it could potentially be useful to us for filling in FIPS codes that the geocoder misses, or validating the geocoded FIPS codes.


## Related Tables

- `fyi__private__projects`
- `census__county_fips`
- `census__state_fips`

## Notes

This table is intended for non-public use.
