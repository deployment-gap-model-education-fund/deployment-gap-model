# census__county_fips

This reference table provides county-equivalent FIPS records used throughout the warehouse and data mart. It is sourced from Census data. County FIPS codes are five digits and can have leading zeroes.

## Table Details

**Source:** Census FIPS reference tables

**Grain:** One row per county-equivalent geography

**Primary key column(s):** `county_id_fips`

**Purpose:** Canonical county FIPS, names, centroid fields, land/water area, and tribal land share.

## Related Tables

- `census__state_fips`
