# airtable__offshore_wind_cable_landing_association

This association table links offshore wind projects to cable landing locations.

## Table Details

**Source:** Madrone Airtable offshore wind base

**Grain:** One row per project-location link

**Primary key column(s):** `project_id`, `location_id`

**Purpose:** Many-to-many bridge between projects and cable landing locations.

## Related Tables

- `airtable__offshore_wind_projects`
- `airtable__offshore_wind_locations`
