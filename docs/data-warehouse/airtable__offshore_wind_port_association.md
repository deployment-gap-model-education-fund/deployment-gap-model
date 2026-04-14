# airtable__offshore_wind_port_association

This association table links offshore wind projects to port locations.

## Table Details

**Source:** Madrone Airtable offshore wind base

**Grain:** One row per project-location link

**Primary key column(s):** `project_id`, `location_id`

**Purpose:** Many-to-many bridge between projects and port locations.

## Transformations

The ETL creates this association table by splitting the comma-separated port location IDs stored on the offshore wind project records, exploding them into one row per project-location pair, and casting `location_id` to an integer.

## Related Tables

- `airtable__offshore_wind_projects`
- `airtable__offshore_wind_locations`
