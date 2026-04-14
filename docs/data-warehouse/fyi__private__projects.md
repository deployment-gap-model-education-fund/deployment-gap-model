# fyi__private__projects

This private warehouse table contains attributes about projects in the interconnection.fyi queue. Tranformations include mapping project status to categorized values.

## Table Details

**Source:** interconnection.fyi queues

**Grain:** One row per queue project

**Primary key column(s):** `project_id`

**Purpose:** Core interconnection queue attributes, status fields, and Madrone actionability flags.

## Related Tables

- `fyi__private__locations`
- `fyi__private__resource_capacity`

## Notes

This table is intended for non-public use.
