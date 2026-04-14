# acp__private__projects

This private warehouse table contains the most recent ACP project snapshot and operational characteristics about those projects. It is updated irregularly in an ad-hoc manner and is used to augment the EIA data to provide a view of projects that are currently operational.

## Table Details

**Source:** American Clean Power projects

**Grain:** One row per current ACP project phase

**Primary key column(s):** `proj_id`

**Purpose:** Most recent snapshot of ACP project records with cleaned fields and geospatial enrichment.

## Transformations

The ETL selects the most recent ACP snapshot, converts raw column names to snake_case, and creates a stable surrogate `proj_id` from project name, phase name, resource type, capacity, states, and counties.

It standardizes key fields such as project status, resource type, owner type, ISO/RTO name, EIA plant ID, and capacity. Location fields are enriched using both latitude/longitude spatial joins and state/county geocoding, with special handling for multi-valued counties and offshore projects where coordinates may fall outside county boundaries.

## Related Tables

- `acp__private__changelog__projects`

## Notes

This table is intended for non-public use.
