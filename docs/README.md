# Deployment Gap Model

This documentation covers the public BigQuery tables in the `data_mart` dataset of the `dbcp-dev-350818` project.

The Data Mart section contains documentation for each table and each column, as well as modeling decisions that may impact downstream analysis.

## Table Naming Schema

Tables in this repo generally follow this naming pattern:

`source_name__private_flag__time_interval__row_granularity_or_aggregation__table_contents_description`

This is the guiding schema for naming warehouse and mart tables.

### Segment meanings

| Segment | Meaning | Example values |
| ---- | ---- | ---- |
| `source_name` | The upstream source system or source family | `fyi`, `acp`, `airtable`, `census`, `eia860m` |
| `private_flag` | Whether the table is intended for non-public use | `private` |
| `time_interval` | The time cadence or snapshot style represented by the table | `annual`, `monthly`, `quarterly`, `yearly`, `changelog` |
| `row_granularity_or_aggregation` | The unit of observation or aggregation level | `projects`, `generators`, `counties`, `iso_regions` |
| `table_contents_description` | The specific subject or metric captured in the table | `operational_status_codes`, `resource_capacity`, `active_clean_projects_capacity` |

### Examples

- `fyi__private__resource_capacity`
- `acp__private__changelog__projects`
- `eia860m__annual__generators`

This convention is meant to make table names legible at a glance: where the data came from, whether it is public, the time shape, the row grain, and what the table actually contains.
