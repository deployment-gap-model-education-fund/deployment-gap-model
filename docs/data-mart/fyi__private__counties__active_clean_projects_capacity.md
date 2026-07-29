# fyi__private__counties__active_clean_projects_capacity

This private table contains per-county information about the capacity of clean resources which
are active in the interconnection queue. Projects from the interconnection.fyi data are filtered for renewable and battery resources, aggregated up to the county level, and broken out by resource. Each row represents one county.

## Column Descriptions

**Unique Key Column(s):** `county_id_fips`

|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Identifiers|`county_id_fips`|County FIPS ID|Census||
|Technical|`battery_storage_active_capacity_mw`|Total active battery storage project capacity in megawatts for the county|FYI|Capacity is allocated across project locations using the `frac_locations_in_county` column from an intermediary table, before aggregation (see "Multi-Location Projects").|
||`onshore_wind_active_capacity_mw`|Total active onshore wind project capacity in megawatts for the county|FYI|Capacity is allocated across project locations using `frac_locations_in_county` before aggregation.|
||`offshore_wind_active_capacity_mw`|Total active offshore wind project capacity in megawatts for the county|FYI|Capacity is allocated across project locations using `frac_locations_in_county` before aggregation.|
||`solar_active_capacity_mw`|Total active solar project capacity in megawatts for the county|FYI|Capacity is allocated across project locations using `frac_locations_in_county` before aggregation.|
||`total_active_clean_projects_capacity_mw`|Total active clean project capacity in megawatts for the county across battery storage, onshore wind, and solar|derived|Sum of the resource-specific active capacity columns in this table.|

## Transformations

### Active Clean Projects

This table uses data from the three FYI data warehouse tables. It first filters to active queue projects,
then keeps only projects with clean resources (battery, solar, and wind).

### County Level Aggragation

After filtering to only active projects, resource capacity is aggregated to the county level. Projects
are deduplicated when creating the `fyi__private__projects` table so we don't need to worry about
double counting project capacity at this point.

Some projects have multiple candidate locations. In this case a project will show up multiple times
in the `fyi__private__locations` table. The capacity for these projects will be allocated equally across
each county it appears in (note that 99% of projects are in only one county).

Finally, the capacity for each individual resource type is summed to create the `total_active_clean_projects_capacity_mw`
column.
