# census__state_fips

This reference table provides state and territory FIPS records used throughout the warehouse and data mart. State FIPS codes are two digits and can have leading zeroes.

## Table Details

**Source:** Census FIPS reference tables

**Grain:** One row per state or territory

**Primary key column(s):** `state_id_fips`

**Purpose:** Canonical state FIPS, names, and abbreviations.

## Transformations

The ETL deduplicates state records by FIPS code, keeping the shortest available state name when multiple names exist for the same code. It then renames the source fields to `state_id_fips`, `state_name`, and `state_abbrev`.

## Related Tables

- `census__county_fips`
