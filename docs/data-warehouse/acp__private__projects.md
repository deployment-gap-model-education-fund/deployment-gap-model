# acp__private__projects

This private warehouse table contains the most recent ACP project snapshot and operational characteristics about those projects. It is updated irregularly in an ad-hoc manner and is used to augment the EIA data to provide a view of projects that are currently operational.

## Table Details

**Source:** American Clean Power projects

**Grain:** One row per current ACP project phase

**Primary key column(s):** `proj_id`

**Purpose:** Most recent snapshot of ACP project records with cleaned fields and geospatial enrichment.

## Related Tables

- `acp__private__changelog__projects`

## Notes

This table is intended for non-public use.
