# airtable__offshore_wind_locations

This table describes onshore locations associated with manually curated offshore wind projects.

## Table Details

**Source:** Madrone Airtable offshore wind

**Grain:** One row per offshore wind location

**Primary key column(s):** `location_id`

**Purpose:** Location records used for ports, cable landing points, and staging areas, with county/state enrichment.

## Transformations

The ETL renames Airtable location fields into warehouse column names, validates that each `location_id` is unique, and geocodes the raw city/county/state values to add county FIPS and locality metadata.

Locations use a two-pass geocoding strategy. The first pass geocodes a combined city-county string to reduce ambiguity, and the second pass retries remaining missing locations using only the raw city and state values.

## Related Tables

- `airtable__offshore_wind_projects`
- `airtable__offshore_wind_cable_landing_association`
- `airtable__offshore_wind_port_association`
- `airtable__offshore_wind_staging_association`
