# iso_projects_change_log

Derived from iso_projects_long_format, this table contains a row for any time a projects enters an interconnection queue, is withdrawn or becomes operational.
A project (iso_region, queue_id) can appear once or twice in this table, once for the time the project enteres the queue and a second time the project is withdrawn or becomes operational. effective_date are end_date create the time interval the value of queue_status represented the status of the project. The end_date is null if the record contains the most recent information.

This table only includes data for ISOs: CAISO, PJM, MISO, SPP, NYISO and ISONE. Currently ERCOT and non ISO regions do not provide operational and withdrawn dates for projects which are required to create this table.

Also, not all ISOs in this table provide withdrawn and operational dates for projects. If a project is missing the date columns for the it's corresponding status, it is removed. >90% of projects missing withdrawn and operational dates a from the early 2000s. This suggests the ISOs were not tracking this information in the early years of the datasets. Here are the number of projects missing the relevant status dates for each ISO region:

### Operational date coverage
| ISO Region | Percent of operational projects missing operational date |
|------------|----------------------|
| CAISO      | 0.00                 |
| ISONE      | 1.00                 |
| MISO       | 0.00                 |
| NYISO      | 14.00                |
| PJM        | 0.40                 |
| SPP        | 8.20                 |


## Withdrawn date coverage
| ISO Region | Percent of withdrawn projects missing withdrawn date |
|------------|----------------------|
| CAISO      | 2.20                 |
| ISONE      | 13.86                |
| MISO       | 1.54                 |
| NYISO      | 8.90                 |
| PJM        | 3.88                 |
| SPP        | 29.37                |


## Column Descriptions

|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Identifiers|`project_id`|A ID assigned to each project. Unique within each source.|derived||
||`source`|"iso" or "proprietary" (for offshore wind projects)|derived||
||`surrogate_id`|An incrementing number as a convenient alternative to the 4-member compound key.|derived||
||`resource_clean`|Fuel type|LBNL||
|Location|`state`|US State name|Census||
||`county`|County name|Census||
||`state_id_fips`|State FIPS ID|Census||
||`county_id_fips`|County FIPS ID|Census||
|Properties|`project_name`|Name of the project|LBNL||
||`is_hybrid`|True/False indicator of whether the project has both generation and storage|derived from LBNL||
||`is_actionable`|True/False indicator of whether the project development process is in the actionable zone.|derived from LBNL||
||`is_nearly_certain`|True/False indicator of whether the project development process is in the actionable zone or nearly completed.|derived from LBNL||
||`resource_class`|Renewable, fossil, or storage|derived from LBNL||
||`iso_region`|Name of the ISO region containing the project. Non-ISO projects are categorized as either Northwest or Southeast by LBNL|LBNL||
||`entity`|Similar to iso_region, but non-ISO projects are identified by utility|LBNL||
||`utility`|The utility within which the project will operate.|LBNL||
||`interconnection_status`|LBNL's status category for the interconnection agreement ("not started", "in progress", "IA executed")|LBNL||
||`point_of_interconnection`|The name of the substation where the plant connects to the grid|LBNL||
||`developer`|The name of the project developer.|LBNL|mostly missing|
||`queue_status`|These project have already been filtered to proposed projects, so this column is all "active".|LBNL||
|Dates|`date_proposed_online`|The date the developer expects the project to be completed.|LBNL||
||`date_entered_queue`|The date the project entered the ISO queue.|LBNL||
|Technical|`capacity_mw`|Export capacity of the generator or storage facility, in megawatts. |LBNL||
||`co2e_tonnes_per_year`|Estimate of annual equivalent CO2 emissions of proposed gas plants, in metric tonnes.|derived from LBNL||
||`frac_locations_in_county`|Fraction of this project's total locations in this county.|derived||
|Regulatory|`ordinance_text`|Summary text of the local ordinances in the given county, if any.|RELDI||
||`ordinance_via_reldi`|True when a county has banned wind or solar development according to RELDI's ordinance database.|derived from RELDI||
||`ordinance_earliest_year_mentioned`|Approximate year the local ordinance was enacted. This was automatically extracted from the ordinance text so is not perfectly accurate.|derived from RELDI||
||`ordinance_jurisdiction_name`|Name of the jurisdiction with a local ordinance. This is usually a county or town within that county. "multiple" if more than one jurisdiction within the county has an ordinance.|RELDI||
||`ordinance_jurisdiction_type`|Category of jurisdiction: county, town, or city. "multiple" if more than one jurisdiction type within the county has an ordinance.|derived from RELDI||
||`ordinance_via_solar_nrel`|True when a county has banned solar development according to NREL's ordinance database.|NREL|See 'NREL Ordinance Interpretation' section below|
||`ordinance_via_wind_nrel`|True when a county has banned wind development according to NREL's ordinance database.|NREL|See 'NREL Ordinance Interpretation' section below|
||`ordinance_via_nrel_is_de_facto`|True when a wind/solar ban is based on technical criteria like setback distances, as opposed to an outright ban.|NREL|See 'NREL Ordinance Interpretation' section below|
||`ordinance_via_self_maintained`|True when a county has banned wind development according to internal data.|proprietary||
||`ordinance_is_restrictive`|Same as `ordinance_via_self_maintained`, but replace `NULL` values with True when *any* of `ordinance_via_solar_nrel`, `ordinance_via_wind_nrel`, or `ordinance_via_reldi` are True|proprietary/NREL/RELDI||
||`state_permitting_text`|Summary text of the wind permitting rules of the given state.|NCSL||
||`state_permitting_type`|Category of the state's wind permitting jurisdiction: state, local, or hybrid.|NCSL||
||`end_date`|Last date the record was valid. Null if the record is currently valid.|LBNL/GS||
||`effective_date`|First date the record was valid. The date the record entered or left the queue.|LBNL/GS||
