# acp__private__changelog__projects

This private warehouse table contains historical ACP project versions across snapshots. A row is emitted whenever a characteristic of a project changes.

## Table Details

**Source:** American Clean Power projects

**Grain:** One row per changed project version per report date

**Primary key column(s):** `proj_id`, `report_date`

**Purpose:** Historical change log showing how ACP project records evolve over time.

## Related Tables

- `acp__private__projects`

## Notes

This table is intended for non-public use.
