# fyi__private__projects

This private warehouse table contains attributes about projects in the interconnection.fyi queue. Transformations include mapping project status to categorized values.

## Table Details

**Source:** interconnection.fyi queues

**Grain:** One row per queue project

**Primary key column(s):** `project_id`

**Purpose:** Core interconnection queue attributes, status fields, and Madrone actionability flags.

## Transformations

The ETL renames source fields, validates interconnection.fyi status values, parses date columns, strips whitespace, standardizes queue status to lowercase, and normalizes ISO naming in selected fields.

It deduplicates likely duplicate physical projects using point of interconnection, capacity, location, utility, resource, and queue status, with tiebreakers based on completion date, status rank, and queue date. It also derives `queue_year`, `is_actionable`, and `is_nearly_certain`, and fills a small expected set of missing queue statuses as withdrawn.

## Related Tables

- `fyi__private__locations`
- `fyi__private__resource_capacity`

## Notes

This table is intended for non-public use.
