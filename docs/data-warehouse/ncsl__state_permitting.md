# ncsl__state_permitting

This table summarizes whether wind siting and permitting authority is primarily state, local, or hybrid.

## Table Details

**Source:** NCSL state permitting

**Grain:** One row per state

**Primary key column(s):** `state_id_fips`

**Purpose:** State-level permitting authority classification and supporting description/link.

## Transformations

The ETL standardizes null-like permitting values, normalizes the District of Columbia state name, applies a manual correction for Mississippi's permitting classification, casts fields to consistent string/categorical dtypes, and adds `state_id_fips`.

The transform validates that state names and permitting-type categories are within expected sets.

## Related Tables

- `census__state_fips`
