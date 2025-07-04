# projects_status_yearly_eia860m

This table contains three years of operational status history for each generator in EIA 860m. The data are yearly timeseries of the operational status code. Each row is a generator-year pair.

## Column Descriptions

**Unique Key Column(s):** (`plant_id_eia`, `generator_id`, `year_start`, `year_end`)

|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Identifiers|`plant_id_eia`|The EIA Plant ID number: a unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.|EIA||
||`generator_id`|The EIA Generator ID string|EIA|Generator ID is usually numeric, but sometimes includes letters. Make sure you treat it as a string!|
||`year_start`|The date of the first day of each year|EIA||
||`year_end`|The date of the last day of each year|EIA||
|Properties|`operational_status_code`|The operational status code defined internally|derived|See the table below for more details|
||`plant_name_eia`|The name of the plant|EIA||

## Operational Status Codes

See the documentation for `projects_status_codes_860m` for a description of the operational status codes.

{% content-ref url="./projects_status_codes_860m.md" %}
[projects_status_codes_860m.md](./projects_status_codes_860m.md)
{% endcontent-ref %}
