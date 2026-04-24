# airtable__manual_ordinances

This table captures manually maintained county-level ordinance flags that supplement external ordinances datasets.

## Table Details

**Source:** Madrone Airtable manual ordinances

**Grain:** One row per county

**Primary key column(s):** `county_id_fips`

**Purpose:** Indicates whether the self-maintained ordinance workflow marks a county as having an ordinance or moratorium.

## Transformations

The ETL queries the Airtable-derived BigQuery table and reduces the raw ordinance status field to a county-level boolean column, `ordinance_via_self_maintained`.

`ordinance_via_self_maintained` is set to `true` when the raw status indicates either an ordinance or moratorium change in process or a prohibitive ordinance/moratorium. It is set to `false` for other non-null statuses and left null when the source status is null.

## Notes

This ordinance data has since been de-prioritized and is used as a reference.
