# county_long_format

This table provides county-level aggregates by facility type: existing power, proposed power, or proposed fossil infrastructure. Each row represents a county-facility_type combination. Local ordinance and state wind permitting information has been joined on for convenience.
The data sources are the LBNL compiled ISO queues, EIP fossil infrastructure, PUDL power plant data, plus Columbia local opposition and NCSL state wind permitting types.

## Modeling Decisions

Almost all the decisions from the ISO and fossil infrastructure project level tables are inherited by these aggregates. The following are in addition to, not instead of, those decisions.

### Local Ordinance Resolution Mismatch

See the description in the iso\_projects\_long\_format section for details.

{% content-ref url="../iso\_projects\_long\_format.md" %}
[iso\_projects\_long\_format.md](../iso\_projects\_long\_format.md)
{% endcontent-ref %}

When aggregating to the county level, 8 out of 92 (9%) counties (as of January 2022) have multiple associated ordinances. In those cases, the ordinance descriptions have been concatenated together.

### EIP Emissions Aggregates

EIP tracks 7 different types of emissions: CO2e, PM2.5, NOx, VOC, SO2, CO, HAPs. For the sake of simplicity, this table contains only:

* CO2e, because of its direct climate relevance and for comparison with power plants
* PM2.5, because of its outsize impact on public health and the EPA’s damage assessments
* NOx, another well-known combustion byproduct

### EIP Project Filtering

EIP’s project database also contains older projects that are already completed or under construction. To keep this table forward looking, those older projects have been removed from these aggregates. This leaves only 136/439 (31%) of the projects as of January 2022 data.
