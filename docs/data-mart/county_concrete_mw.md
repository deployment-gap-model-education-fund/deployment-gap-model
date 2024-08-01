# county_concrete_mw

This table contains county-level aggregates of in-progress power generation projects.

## Column Descriptions

**Unique Key Column(s):** (`county_id_fips`, `resource_type`, `iso_rto`)
|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Identifiers|`resource_type`|Fuel or project type|EIA 860m or ACP||
||`iso_rto`|The ISO/RTO the project will belong to. Usually 1:1 with county, but border counties may have multiple ISO/RTOs|EIA 860m or ACP||
||`county_id_fips`|County FIPS ID|Census||
|Location|`state_id_fips`|State FIPS ID|Census||
||`state`|State Name|Census||
||`county`|County Name|Census||
|Technical|`capacity_under_construction_mw`|Total Capacity in MW of projects that have started but not completed construction|EIA 860m or ACP||
||`capacity_awaiting_permitting_mw`|Total capacity in MW of projects that have not yet started construction|EIA 860m or ACP||
||`capacity_total_proposed_mw`|Total capacity in either stage (sum of the other two capacity columns)|EIA 860m or ACP||

## Modeling Decisions

### Prioritizing EIA 860m over ACP Data

The EIA 860m and ACP data have approximately 40% overlap in the projects they report (note that this includes existing projects, which are filtered out from this table). When merging the two, I excluded ACP projects that had an EIA plant ID that matched an ID in the EIA 860m data. This prioritizes EIA data over ACP data. One limitation of this method is that some ACP projects may truly exist in the EIA data but are missing the plant ID, which would result in double counting.

### Multiple Project Counties

About 0.5% of ACP projects are associated with multiple counties. For the sake of simplicity, I reduced this to one county via the following method:

1. Use the county containing the provided lat/lon coordinate (if available)
1. Use the first county listed in the ACP data

### Project Status

The `capacity_under_construction_mw` contains ACP projects with the status "Under Construction" and EIA 860m projects with `operational_status_code` equal to 4, 5, or 6, which correspond to EIA status codes U, V, or TS. The `capacity_awaiting_permitting_mw` contains ACP projects with the status "Advanged Development" and EIA 860m projects with `operational_status_code` equal to 1, 2 or 3, which correspond to EIA status codes P, L, or T. The `capacity_total_proposed_mw` is the sum of the other two columns.

See the documentation for `projects_status_codes_860m` for more details about EIA operational status codes.

{% content-ref url="./projects_status_codes_860m.md" %}
[projects_status_codes_860m.md](./projects_status_codes_860m.md)
{% endcontent-ref %}
