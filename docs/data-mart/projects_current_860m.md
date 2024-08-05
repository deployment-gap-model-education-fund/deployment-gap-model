# projects_current_860m

This table contains the most recent information from EIA 860m for each generator. Each row represents one generator.

## Column Descriptions

**Unique Key Column(s):** (`plant_id_eia`, `generator_id`)

|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Identifiers|`plant_id_eia`|The EIA Plant ID number|EIA||
||`generator_id`|The EIA Generator ID string|EIA||
|Properties|`plant_name_eia`|The name of the plant|EIA||
||`utility_id_eia`|The EIA Utility ID number|EIA||
||`utility_name_eia`|The name of the utility|EIA||
||`capacity_mw`|The generator's capacity in megawatts|EIA||
||`fuel_type_code_pudl`|The PUDL fuel type code|PUDL||
||`prime_mover_code`|The EIA prime mover code|EIA||
||`operational_status_code`|The operational status code defined internally|derived|See the notes below for more details|
||`operational_status_category`|Operational status simplified to one of `proposed`, `existing`, or `retired`|derived||
||`raw_operational_status_code`|The original EIA operational status code|EIA||
||`current_planned_generator_operating_date`|The date the generator is expected to begin operating|EIA||
||`energy_source_code_1`|The EIA energy source code|EIA||
||`energy_storage_capacity_mwh`|The generator's energy storage capacity in megawatt-hours|EIA||
||`generator_retirement_date`|The date the generator is expected to retire|EIA||
||`net_capacity_mwdc`|The generator's net capacity in megawatts DC|EIA||
||`planned_derate_date`|The date the generator is expected to be derated|EIA||
||`planned_generator_retirement_date`|The date the generator is expected to retire|EIA||
||`planned_net_summer_capacity_derate_mw`|The generator's planned derated capacity in megawatts|EIA||
||`planned_net_summer_capacity_uprate_mw`|The generator's planned uprated capacity in megawatts|EIA||
||`planned_uprate_date`|The date the generator is expected to be uprated|EIA||
||`technology_description`|The EIA technology description|EIA||
||`report_date`|The date the data were reported|EIA||
|Location|`state`|The state in which the generator is located|EIA||
||`county`|The county in which the generator is located|EIA||
||`state_id_fips`|The FIPS code for the state in which the generator is located|EIA||
||`county_id_fips`|The FIPS code for the county in which the generator is located|EIA||
||`latitude`|The latitude of the generator|EIA||
||`longitude`|The longitude of the generator|EIA||

## Operational Status Codes

See the documentation for `projects_status_codes_860m` for a description of the operational status codes.

{% content-ref url="./projects_status_codes_860m.md" %}
[projects_status_codes_860m.md](./projects_status_codes_860m.md)
{% endcontent-ref %}
