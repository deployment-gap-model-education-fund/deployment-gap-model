# census__county_fips

This reference table provides county-equivalent FIPS records used throughout the warehouse and data mart. It is sourced from Census data. County FIPS codes are five digits and can have leading zeroes.

## Table Details

**Source:** Census FIPS reference tables

**Grain:** One row per county-equivalent geography

**Primary key column(s):** `county_id_fips`

**Purpose:** Canonical county FIPS, names, centroid fields, land/water area, and tribal land share.

## Transformations

The ETL renames Census shapefile fields into warehouse-friendly names, converts land and water area from square meters to square kilometers, and casts centroid coordinates into numeric columns.

It also calculates `raw_tribal_land_frac` and `tribal_land_frac` by spatially intersecting county geometries with Census tribal land geometries. `tribal_land_frac` is capped at 1.0 and rounded to two decimal places.

## Related Tables

- `census__state_fips`
