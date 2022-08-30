# existing_plants

This table describes existing power plants from the PUDL database (mostly EIA data). Each row represents one plant.

## Column Descriptions

**Unique Key Column(s):** `plant_id_eia`

|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Identifiers|`plant_id_eia`|Unique plant identifier from the EIA|PUDL||
|Location|`state`|US State name|PUDL||
||`county`|County name|PUDL||
||`state_id_fips`|State FIPS ID|Census||
||`county_id_fips`|County FIPS ID|Census||
|Operational|`resource`|Primary fuel type|PUDL||
||`max_operating_date`|The date of the last major change to the plant.|derived from PUDL||
||`capacity_mw`|Generation capacity in megawatts|PUDL||
|Emissions|`co2e_tonnes_per_year`|Estimated annual equivalent CO2 emissions, in metric tonnes.|derived from PUDL|

## Modeling Decisions

### Multiple Fuel Types

Many power plants have multiple generators and about 11% of plants have generators that run on different fuels (such as a co-located coal plant and natural gas turbine). To simplify this table, plants are classified by only one fuel type: the fuel type responsible for the largest share of electrical generation. Where generation data is not available, the fuel type with the largest capacity is used.
