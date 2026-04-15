# eia860m__operational_status_codes

This table maps EIA operational status codes to their code numbers and status descriptions.

## Table Details

**Source:** PUDL / EIA 860M

**Grain:** One row per operational status code

**Primary key column(s):** `code`

**Purpose:** Lookup table for status code, status code numbers, and status code descriptions.

## Transformations

The ETL starts with PUDL's operational status code descriptions and joins them to Madrone's numeric status scale. The resulting `status` value orders proposed, operating, and retired statuses for downstream status comparisons.

## Related Tables

- `eia860m__annual__generators`
- `_eia860m__changelog__generators`
