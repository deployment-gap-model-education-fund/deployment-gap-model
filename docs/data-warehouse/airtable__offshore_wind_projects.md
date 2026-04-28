# airtable__offshore_wind_projects

This table describes manually curated offshore wind projects from Madrone's Airtable offshore wind base.

## Table Details

**Source:** Madrone Airtable offshore wind

**Grain:** One row per offshore wind project

**Primary key column(s):** `project_id`

**Purpose:** Core project attributes including developer, capacity, status, and DBCP actionability flags.

## Transformations

The ETL renames Airtable fields into snake_case warehouse columns, casts key fields like project IDs, capacity, and proposed completion year into typed columns, and validates that project IDs and project names are unique.

The transform also maps construction statuses into a queue-style `queue_status` field (`active`, `completed`, or `withdrawn`) so offshore wind projects can be compared more easily with queue-based project sources. It derives `is_actionable` and `is_nearly_certain` flags from construction status.

## Related Tables

- `airtable__offshore_wind_locations`
- `airtable__association__offshore_wind_cable_landing`
- `airtable__offshore_wind_port_association`
- `airtable__offshore_wind_staging_association`
