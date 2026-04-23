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
||`solar_active_capacity_mw`|Total active solar project capacity in megawatts for the county|FYI|Capacity is allocated across project locations using `frac_locations_in_county` before aggregation.|
||`total_active_clean_projects_capacity_mw`|Total active clean project capacity in megawatts for the county across battery storage, onshore wind, and solar|derived|Sum of the resource-specific active capacity columns in this table.|

## Modeling Decisions

### Active Clean Projects

This table starts from `fyi_projects_long_format` filtered to active queue projects, then keeps clean resources from the FYI resource mapping. The private output currently includes battery storage, onshore wind, and solar as separate resource columns.

### Multi-Location Projects

Some projects have multiple candidate locations. In an intermediary table (`fyi_projects_long_format`) which is not persisted, project capacity is multiplied by `frac_locations_in_county` before county aggregation so capacity is apportioned across listed counties instead of counted in full for every location.

### Counties Included

The output is reindexed to all counties that have at least one active clean FYI project in the intermediate long county-resource table. Counties without battery storage, onshore wind, or solar capacity may have null resource-specific capacity values.
