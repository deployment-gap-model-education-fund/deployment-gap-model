# eia860m__operational_status_codes

This table maps raw EIA 860M operational status codes to Madrone's internal numeric status scale and status descriptions.

## Table Details

**Source:** PUDL / EIA 860M

**Grain:** One row per operational status code

**Primary key column(s):** `code`

**Purpose:** Lookup table for raw EIA operational status codes, Madrone status values, and descriptions used in downstream EIA 860M warehouse and data mart tables.

## Column Descriptions

|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Identifiers|`code`|Raw EIA 860M operational status code|PUDL / EIA|Primary key. Examples include `P`, `L`, `T`, `U`, `V`, `TS`, `OP`, `RE`, `IP`, and `OT`.|
|Status|`status`|Madrone numeric operational status value|derived|Orders proposed, under-construction, operating, retired, canceled, and other statuses for downstream status comparisons.|
||`description`|Detailed operational status description from the EIA code table|PUDL / EIA||
||`summarized_status_description`|Short grouped description for the Madrone numeric status value|derived|Multiple raw EIA codes can share a summarized description when they map to the same Madrone status value.|

## Transformations

The ETL starts with PUDL's operational status code descriptions and joins them to Madrone's numeric status scale. The resulting `status` value orders proposed, under-construction, operating, retired, canceled, and other statuses for downstream status comparisons.

The transform also adds `summarized_status_description`, which groups raw EIA code descriptions by the internal Madrone status value.

## Madrone Status Scale

|Status|Meaning|
|----|----|
|`1`|Planned for installation, regulatory approvals not initiated, and not under construction|
|`2`|Regulatory approvals pending; not under construction but site preparation could be underway|
|`3`|Regulatory approvals received; not under construction but site preparation could be underway|
|`4`|Under construction, less than or equal to 50 percent complete|
|`5`|Under construction, more than 50 percent complete|
|`6`|Construction complete, but not yet in commercial operation|
|`7`|Operational|
|`8`|Retired|
|`98`|Canceled, indefinitely postponed, or no longer in a resource plan|
|`99`|Other or unknown|

## Related Tables

- `_eia860m__changelog__generators`
- `eia860m__latest__generators`
- `eia860m__monthly__generators`
- `eia860m__generators_operational_status_transition_dates`
