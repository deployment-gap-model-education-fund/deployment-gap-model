# fyi__private__locations

This private warehouse table contains normalized location data for interconnection.fyi queue projects. We use a geocoder to validate county locational information and FIPS codes, then join it onto the Census county FIPS backbone.

## Table Details

**Source:** interconnection.fyi queues

**Grain:** One row per project-location record

**Primary key column(s):** `project_id`

**Purpose:** County/state/location details for projects with location data.

## Related Tables

- `fyi__private__projects`
- `census__county_fips`
- `census__state_fips`

## Notes

This table is intended for non-public use.
