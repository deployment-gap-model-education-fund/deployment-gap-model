# airtable__offshore_wind_port_association

This association table links offshore wind projects to port locations.

## Table Details

**Source:** Madrone Airtable offshore wind base

**Grain:** One row per project-location link

**Primary key column(s):** `project_id`, `location_id`

**Purpose:** Many-to-many bridge between projects and port locations.

## Related Tables

- `airtable__offshore_wind_projects`
- `airtable__offshore_wind_locations`
