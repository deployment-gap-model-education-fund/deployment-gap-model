# fyi__private__resource_capacity

This private warehouse table contains normalized resource and capacity records for interconnection.fyi queue projects. Transformations include mapping project resources types to clean, categorized values.

## Table Details

**Source:** interconnection.fyi queues

**Grain:** One row per project-resource pair

**Primary key column(s):** `project_id`

**Purpose:** Resource-level capacity breakout for projects with one or more generation types.

## Transformations

The ETL parses interconnection.fyi's capacity-by-generation-type breakout where available, explodes multi-resource projects into one row per project-resource pair, and sums duplicate resource rows for the same project when appropriate.

For projects without a parsed resource-capacity breakout, the transform falls back to the project-level capacity and canonical generation type. It then maps raw resource values into the cleaned `resource_clean` categories used downstream.

## Related Tables

- `fyi__private__projects`

## Notes

This table is intended for non-public use.
