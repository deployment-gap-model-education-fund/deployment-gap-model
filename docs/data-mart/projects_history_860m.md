# projects_history_860m

This table contains three years of operational status history for each generator in EIA 860m. The data are quarterly timeseries of the operational status code. Each row is a generator-quarter pair.

## Column Descriptions

**Unique Key Column(s):** (`plant_id_eia`, `generator_id`, `quarter_end`)

|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Identifiers|`plant_id_eia`|The EIA Plant ID number|EIA||
||`generator_id`|The EIA Generator ID string|EIA||
||`quarter_end`|The date of the last day of each quarter|EIA||
|Properties|`operational_status_code`|The operational status code defined internally|derived|See the table below for more details|
||`plant_name_eia`|The name of the plant|EIA||

## Operational Status Codes

The EIA defines 12 status codes for generators, but we have simplified them down to 9 numbers, in the order a generator would typically go through in its lifecycle. The codes are:

|Code|EIA Code|Description|
|----|----|----|
|1|P|Planned for installation but regulatory approvals not initiated; Not under construction|
|2|L|Regulatory approvals pending. Not under construction but site preparation could be underway|
|3|T|Regulatory approvals received. Not under construction but site preparation could be underway|
|4|U|Under construction, less than or equal to 50 percent complete (based on construction time to date of operation)|
|5|V|Under construction, more than 50 percent complete (based on construction time to date of operation)|
|6|TS|Construction complete, but not yet in commercial operation|
|7|OA, OP, OS, SB|Various operational categories|
|8|RE|Retired|
|98|IP|Planned new generator canceled, indefinitely postponed, or no longer in resource plan|
|99|OT|Other|
