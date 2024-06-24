# projects_transition_dates_860m

This table contains the dates of each status transition for each generator in EIA 860m. Each row is one generator.

## Column Descriptions

**Unique Key Column(s):** (`plant_id_eia`, `generator_id`)

|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Identifiers|`plant_id_eia`|The EIA Plant ID number|EIA||
||`generator_id`|The EIA Generator ID string|EIA||
|Properties|`plant_name_eia`|The name of the plant|EIA||
||`date_entered_1`|The date the generator entered status code 1|EIA||
||`date_entered_2`|The date the generator entered status code 2|EIA||
||`date_entered_3`|The date the generator entered status code 3|EIA||
||`date_entered_4`|The date the generator entered status code 4|EIA||
||`date_entered_5`|The date the generator entered status code 5|EIA||
||`date_entered_6`|The date the generator entered status code 6|EIA||
||`date_entered_7`|The date the generator entered status code 7|EIA||
||`date_entered_8`|The date the generator entered status code 8|EIA||
||`date_entered_99`|The datethe generator entered status code 99|EIA||

## Operational Status Codes

See the documentation for `projects_status_codes_860m` for a description of the operational status codes.

{% content-ref url="./projects_status_codes_860m.md" %}
[projects_status_codes_860m.md](./projects_status_codes_860m.md)
{% endcontent-ref %}
