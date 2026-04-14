# fyi__private__resource_capacity

This private warehouse table contains normalized resource and capacity records for interconnection.fyi queue projects. Transformations include mapping project resources types to clean, categorized values.

## Table Details

**Source:** interconnection.fyi queues

**Grain:** One row per project-resource pair

**Primary key column(s):** `project_id`

**Purpose:** Resource-level capacity breakout for projects with one or more generation types.

## Related Tables

- `fyi__private__projects`

## Notes

This table is intended for non-public use.
