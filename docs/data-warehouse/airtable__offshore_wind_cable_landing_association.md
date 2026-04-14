# airtable__offshore_wind_cable_landing_association

This association table links offshore wind projects to cable landing locations.

## Table Details

**Source:** Madrone Airtable offshore wind base

**Grain:** One row per project-location link

**Primary key column(s):** `project_id`, `location_id`

**Purpose:** Many-to-many bridge between projects and cable landing locations.

## Transformations

The ETL creates this association table by splitting the comma-separated cable landing location IDs stored on the offshore wind project records, exploding them into one row per project-location pair, and casting `location_id` to an integer.

## Related Tables

- `airtable__offshore_wind_projects`
- `airtable__offshore_wind_locations`
