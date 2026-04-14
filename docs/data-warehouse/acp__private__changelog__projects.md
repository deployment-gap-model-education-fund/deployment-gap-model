# acp__private__changelog__projects

This private warehouse table contains historical ACP project versions across snapshots. A row is emitted whenever a characteristic of a project changes.

## Table Details

**Source:** American Clean Power projects

**Grain:** One row per changed project version per report date

**Primary key column(s):** `proj_id`, `report_date`

**Purpose:** Historical change log showing how ACP project records evolve over time.

## Transformations

The ETL applies the same cleaning and location enrichment as `acp__private__projects` across all available ACP snapshots. It then hashes the non-raw comparison columns within each `proj_id` and emits a new row only when a cleaned project record changes relative to its previous snapshot.

The transform adds `valid_until_date` by shifting the next report date within each `proj_id`, which makes the table usable as a project-version history.

## Related Tables

- `acp__private__projects`

## Notes

This table is intended for non-public use.
