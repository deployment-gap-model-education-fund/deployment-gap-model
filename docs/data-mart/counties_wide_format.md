# counties_wide_format

This table is mostly a restructured version of counties_long_format so that each row represents a whole county. The only other difference is the addition of two columns `offshore_wind_capacity_mw_via_ports` and `offshore_wind_interest_type`. See below for details.

## Column Descriptions

**Unique Key Column(s):** `county_id_fips`

Aggregates of fossil infrastructure projects only include pre-construction projects (as of the data release date, see the EIP data source page for details). This provides a forward-looking lens to the data.

{% content-ref url="../sources/README.md" %}
[Data Sources](../sources/README.md)
{% endcontent-ref %}

Fossil generation aggregates include coal, oil, and gas power plants.

"renewable_and_battery" aggregates include solar and wind power plants **and** battery storage facilities.

|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Identifiers|`county_id_fips`|County FIPS ID|Census||
|Location|`county`|County name|Census||
||`state_id_fips`|State FIPS ID|Census||
||`state`|US State name|Census||
|Resource Aggregates|`county_total_co2e_tonnes_per_year`|Total CO2e emissions, across all sources and both proposed and existing, in metric tonnes per year.|PUDL, LBNL, EIP||
||`fossil_existing_capacity_mw`|Generation capacity, in megawatts, of existing fossil power plants.|PUDL||
||`fossil_existing_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from existing fossil power plants, in metric tonnes.|derived from PUDL||
||`fossil_existing_facility_count`|Number of existing fossil power plants.|PUDL||
||`fossil_proposed_capacity_mw`|Generation capacity, in megawatts, of proposed fossil power plants.|LBNL||
||`fossil_proposed_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from proposed fossil power plants, in metric tonnes.|derived from LBNL||
||`fossil_proposed_facility_count`|Number of proposed fossil power plants.|LBNL||
||`renewable_and_battery_existing_capacity_mw`|Combined generation and storage capacity, in megawatts, of existing renewable power and storage facilities.|PUDL||
||`renewable_and_battery_existing_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from renewable generation and battery storage facilities, in metric tonnes.|derived from PUDL||
||`renewable_and_battery_existing_facility_count`|Number of existing renewable generation or battery storage facilities.|PUDL||
||`renewable_and_battery_proposed_capacity_mw`|Combined generation and storage capacity, in megawatts, of proposed renewable power and storage facilities.|LBNL||
||`renewable_and_battery_proposed_facility_count`|Number of proposed renewable generation or battery storage facilities.|LBNL||
||`renewable_and_battery_proposed_avoided_co2e_tonnes_per_year`|Estimated avoided CO2 equivalent emissions of proposed projects, in metric tonnes per year.|EPA AVERT||
||`renewable_and_battery_proposed_capacity_mw_actionable`|Combined generation and storage capacity, in megawatts, of proposed renewable power and storage facilities in the actionable stage of development.|derived from LBNL||
||`renewable_and_battery_proposed_facility_count_actionable`|Number of proposed renewable generation or battery storage facilities in the actionable stage of development.|derived from LBNL||
||`renewable_and_battery_proposed_avoided_co2e_actionable`|Estimated avoided CO2 equivalent emissions of proposed projects in the actionable stage of development, in metric tonnes per year.|EPA AVERT||
||`renewable_and_battery_proposed_capacity_mw_nearly_certain`|Combined generation and storage capacity, in megawatts, of proposed renewable power and storage facilities in the nearly completed stages of development.|derived from LBNL||
||`renewable_and_battery_proposed_facility_count_nearly_certain`|Number of proposed renewable generation or battery storage facilities in the nearly completed stages of development.|derived from LBNL||
||`renewable_and_battery_proposed_avoided_co2e_nearly_certain`|Estimated avoided CO2 equivalent emissions of proposed but nearly completed projects, in metric tonnes per year.|EPA AVERT||
||`infra_total_proposed_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from all proposed fossil infrastructure projects, in metric tonnes.|EIP||
||`infra_total_proposed_facility_count`|Number of proposed fossil infrastructure facilities across all sectors.|EIP||
||`infra_total_proposed_nox_tonnes_per_year`|Annual NOx emissions from all proposed fossil infrastructure projects, in metric tonnes.|EIP||
||`infra_total_proposed_pm2_5_tonnes_per_year`|Annual PM2.5 emissions from all proposed fossil infrastructure projects, in metric tonnes.|EIP||
||`battery_storage_existing_capacity_mw`|Storage capacity, in megawatts, of existing battery storage facilities.|PUDL||
||`battery_storage_existing_facility_count`|Number of existing battery storage facilities.|PUDL||
||`battery_storage_proposed_capacity_mw`|Storage capacity, in megawatts, of proposed battery storage facilities.|LBNL||
||`battery_storage_proposed_facility_count`|Number of proposed battery storage facilities.|LBNL||
||`coal_existing_capacity_mw`|Generation capacity, in megawatts, of existing coal power plants.|PUDL||
||`coal_existing_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from existing coal power plants, in metric tonnes.|derived from PUDL||
||`coal_existing_facility_count`|Number of existing coal power plants.|PUDL||
||`coal_proposed_capacity_mw`|Generation capacity, in megawatts, of proposed coal power plants.|LBNL||
||`coal_proposed_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from proposed fossil power plants, in metric tonnes.|derived from LBNL||
||`coal_proposed_facility_count`|Number of proposed coal power plants.|LBNL||
||`gas_existing_capacity_mw`|Generation capacity, in megawatts, of existing gas power plants.|PUDL||
||`gas_existing_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from existing gas power plants, in metric tonnes.|derived from PUDL||
||`gas_existing_facility_count`|Number of existing gas power plants.|PUDL||
||`gas_proposed_capacity_mw`|Generation capacity, in megawatts, of proposed gas power plants.|LBNL||
||`gas_proposed_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from proposed gas power plants, in metric tonnes.|derived from LBNL||
||`gas_proposed_facility_count`|Number of proposed gas power plants.|LBNL||
||`offshore_wind_existing_capacity_mw`|Generation capacity, in megawatts, of existing offshore wind power plants.|PUDL||
||`offshore_wind_existing_facility_count`|Number of existing offshore wind power plants.|PUDL||
||`offshore_wind_proposed_capacity_mw`|Generation capacity, in megawatts, of propsed offshore wind power plants. When a wind farm has multiple cable landing locations, the capacity is split equally between landing locations. Note that this is the only proposed capacity column NOT sourced from LBNL.|original work||
||`offshore_wind_proposed_facility_count`|Number of proposed offshore wind power plants.|original work||
||`offshore_wind_proposed_avoided_co2e_tonnes_per_year`|Estimated avoided CO2 equivalent emissions of all proposed offshore wind projects, in metric tonnes per year.|EPA AVERT||
||`offshore_wind_capacity_mw_via_ports`|Total generation capacity, in megawatts, of propsed offshore wind power plants with an assembly/manufacturing port in this county. Capacity has NOT been split between multiple port locations, so the sum of this column is deliberately greater than total proposed offshore wind capacity.|original work||
||`offshore_wind_interest_type`|Describes the relationship of this county to offshore wind. One of `Proposed lease area`, `Contracted project`,  `Lease area in proximity`, or NULL.|original work||
||`offshore_wind_proposed_capacity_mw_actionable`|Total capacity, in megawatts, of proposed offshore wind facilities in the actionable stage of development.|EPA AVERT||
||`offshore_wind_proposed_facility_count_actionable`|Number of proposed offshore wind projects in the actionable stage of development.|EPA AVERT||
||`offshore_wind_proposed_avoided_co2e_actionable`|Estimated avoided CO2 equivalent emissions of proposed offshore wind projects in the actionable stage of development, in metric tonnes per year.|EPA AVERT||
||`offshore_wind_proposed_capacity_mw_nearly_certain`|Total capacity, in megawatts, of nearly completed offshore wind facilities.|EPA AVERT||
||`offshore_wind_proposed_facility_count_nearly_certain`|Number of nearly completed offshore wind facilities.|EPA AVERT||
||`offshore_wind_proposed_avoided_co2e_nearly_certain`|Estimated avoided CO2 equivalent emissions of nearly completed offshore wind projects, in metric tonnes per year.|EPA AVERT||
||`oil_existing_capacity_mw`|Generation capacity, in megawatts, of existing oil and diesel power plants.|PUDL||
||`oil_existing_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from existing oil power plants, in metric tonnes.|derived from PUDL||
||`oil_proposed_facility_count`|Number of proposed oil power plants.|LBNL||
||`oil_proposed_capacity_mw`|Generation capacity, in megawatts, of proposed oil and diesel power plants.|LBNL||
||`oil_proposed_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from proposed oil power plants, in metric tonnes.|derived from LBNL||
||`oil_existing_facility_count`|Number of existing oil power plants.|PUDL||
||`onshore_wind_existing_capacity_mw`|Generation capacity, in megawatts, of existing onshore wind power plants.|PUDL||
||`onshore_wind_existing_facility_count`|Number of existing onshore wind power plants.|PUDL||
||`onshore_wind_proposed_capacity_mw`|Generation capacity, in megawatts, of proposed onshore wind power plants.|LBNL||
||`onshore_wind_proposed_facility_count`|Number of proposed onshore wind power plants.|LBNL||
||`onshore_wind_proposed_avoided_co2e_tonnes_per_year`|Estimated avoided CO2 equivalent emissions of all proposed onshore wind projects, in metric tonnes per year.|EPA AVERT||
||`onshore_wind_proposed_capacity_mw_actionable`|Total capacity, in megawatts, of proposed onshore wind facilities in the actionable stage of development.|EPA AVERT||
||`onshore_wind_proposed_facility_count_actionable`|Number of proposed onshore wind projects in the actionable stage of development.|EPA AVERT||
||`onshore_wind_proposed_avoided_co2e_actionable`|Estimated avoided CO2 equivalent emissions of proposed onshore wind projects in the actionable stage of development, in metric tonnes per year.|EPA AVERT||
||`onshore_wind_proposed_capacity_mw_nearly_certain`|Total capacity, in megawatts, of nearly completed onshore wind facilities.|EPA AVERT||
||`onshore_wind_proposed_facility_count_nearly_certain`|Number of nearly completed onshore wind facilities.|EPA AVERT||
||`onshore_wind_proposed_avoided_co2e_nearly_certain`|Estimated avoided CO2 equivalent emissions of nearly completed onshore wind projects, in metric tonnes per year.|EPA AVERT||
||`solar_existing_capacity_mw`|Generation capacity, in megawatts, of existing solar power plants.|PUDL||
||`solar_existing_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from existing solar power plants, in metric tonnes. The few non-zero values of this column come from solar thermal facilities that also burn gas.|derived from PUDL||
||`solar_existing_facility_count`|Number of existing solar power plants.|PUDL||
||`solar_proposed_capacity_mw`|Generation capacity, in megawatts, of proposed solar power plants.|LBNL||
||`solar_proposed_facility_count`|Number of proposed solar power plants.|LBNL||
||`solar_proposed_avoided_co2e_tonnes_per_year`|Estimated avoided CO2 equivalent emissions of all proposed solar projects, in metric tonnes per year.|EPA AVERT||
||`solar_proposed_capacity_mw_actionable`|Total capacity, in megawatts, of proposed solar facilities in the actionable stage of development.|EPA AVERT||
||`solar_proposed_facility_count_actionable`|Number of proposed solar projects in the actionable stage of development.|EPA AVERT||
||`solar_proposed_avoided_co2e_actionable`|Estimated avoided CO2 equivalent emissions of proposed solar projects in the actionable stage of development, in metric tonnes per year.|EPA AVERT||
||`solar_proposed_capacity_mw_nearly_certain`|Total capacity, in megawatts, of nearly completed solar facilities.|EPA AVERT||
||`solar_proposed_facility_count_nearly_certain`|Number of nearly completed solar facilities.|EPA AVERT||
||`solar_proposed_avoided_co2e_nearly_certain`|Estimated avoided CO2 equivalent emissions of nearly completed solar projects, in metric tonnes per year.|EPA AVERT||
||`infra_gas_proposed_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from proposed gas infrastructure, in metric tonnes.|EIP||
||`infra_gas_proposed_facility_count`|Number of existing infrastructure facilities in the gas sector, excluding LNG.|EIP||
||`infra_gas_proposed_nox_tonnes_per_year`|Annual NOx emissions from all proposed gas infrastructure projects, in metric tonnes.|EIP||
||`infra_gas_proposed_pm2_5_tonnes_per_year`|Annual PM2.5 emissions from all proposed gas infrastructure projects, in metric tonnes.|EIP||
||`infra_lng_proposed_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from proposed LNG infrastructure, in metric tonnes.|EIP||
||`infra_lng_proposed_facility_count`|Number of existing infrastructure facilities in the LNG sector.|EIP||
||`infra_lng_proposed_nox_tonnes_per_year`|Annual NOx emissions from all proposed LNG infrastructure projects, in metric tonnes.|EIP||
||`infra_lng_proposed_pm2_5_tonnes_per_year`|Annual PM2.5 emissions from all proposed LNG infrastructure projects, in metric tonnes.|EIP||
||`infra_oil_proposed_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from proposed oil infrastructure, in metric tonnes.|EIP||
||`infra_oil_proposed_facility_count`|Number of existing infrastructure facilities in the oil sector, excluding natural-gas-specific facilties.|EIP||
||`infra_oil_proposed_nox_tonnes_per_year`|Annual NOx emissions from all proposed oil infrastructure projects, in metric tonnes.|EIP||
||`infra_oil_proposed_pm2_5_tonnes_per_year`|Annual PM2.5 emissions from all proposed oil infrastructure projects, in metric tonnes.|EIP||
||`infra_petrochemicals_and_plastics_proposed_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from proposed petrochemical and plastics infrastructure, in metric tonnes.|EIP||
||`infra_petrochemicals_and_plastics_proposed_facility_count`|Number of existing infrastructure facilities in the petrochemicals and plastics sector, excluding fertilizers.|EIP||
||`infra_petrochemicals_and_plastics_proposed_nox_tonnes_per_year`|Annual NOx emissions from all proposed petrochemical and plastics infrastructure projects, in metric tonnes.|EIP||
||`infra_petrochemicals_and_plastics_proposed_pm2_5_tonnes_per_yea`|Annual PM2.5 emissions from all proposed petrochemical and plastics infrastructure projects, in metric tonnes.|EIP||
||`infra_synthetic_fertilizers_proposed_co2e_tonnes_per_year`|Annual CO2 equivalent emissions from proposed fertilizer infrastructure, in metric tonnes.|EIP||
||`infra_synthetic_fertilizers_proposed_facility_count`|Number of existing infrastructure facilities in the fertilizers sector.|EIP||
||`infra_synthetic_fertilizers_proposed_nox_tonnes_per_year`|Annual NOx emissions from all proposed fossil infrastructure projects, in metric tonnes.|EIP||
||`infra_synthetic_fertilizers_proposed_pm2_5_tonnes_per_year`|Annual PM2.5 emissions from all proposed fossil infrastructure projects, in metric tonnes.|EIP||
|Regulatory|`ordinance_text`|Summary text of the local ordinances in the given county, if any.|RELDI||
||`ordinance_via_reldi`|True when a county has banned wind or solar development according to RELDI's ordinance database.|derived from RELDI||
||`ordinance_earliest_year_mentioned`|Approximate year the local ordinance was enacted. This was automatically extracted from the ordinance text so is not perfectly accurate.|derived from RELDI||
||`ordinance_jurisdiction_name`|Name of the jurisdiction with a local ordinance. This is usually a county or town within that county. "multiple" if more than one jurisdiction within the county has an ordinance.|RELDI||
||`ordinance_jurisdiction_type`|Category of jurisdiction: county, town, or city. "multiple" if more than one jurisdiction type within the county has an ordinance.|derived from RELDI||
||`ordinance_via_solar_nrel`|True when a county has banned solar development according to NREL's ordinance database.|NREL|See 'NREL Ordinance Interpretation' section below|
||`ordinance_via_wind_nrel`|True when a county has banned wind development according to NREL's ordinance database.|NREL|See 'NREL Ordinance Interpretation' section below|
||`ordinance_via_nrel_is_de_facto`|True when a wind/solar ban is based on technical criteria like setback distances, as opposed to an outright ban.|NREL|See 'NREL Ordinance Interpretation' section below|
||`ordinance_is_restrictive`|True when any of `ordinance_via_solar_nrel`, `ordinance_via_wind_nrel`, or `ordinance_via_reldi` are True|NREL/RELDI||
||`state_permitting_text`|Summary text of the wind permitting rules of the given state.|NCSL||
||`state_permitting_type`|Category of the state's wind permitting jurisdiction: state, local, or hybrid.|NCSL||
||`ec_qualifies`|True if the county qualifies via employment OR the fraction of qualifying area from coal closures is >= 50%|derived from RMI||
||`ec_coal_closures_area_fraction`|Fraction of county land area that qualifies due to coal mine and generator closures.|RMI||
||`ec_qualifies_via_employment`|True if the county is part of a qualifying Statistical Area based on fossil fuel employment.|RMI||
||`county_land_area_km2`|Total land area of a county with units of square kilometers.|Census TIGER||
||`unprotected_land_area_km2`|Total county area minus protected area (GAP 1 or 2). See Protected Land Area section below.|USGS PAD||
||`federal_fraction_unprotected_land`|Fraction of unprotected land area managed by Federal agencies.|USGS PAD||
||`tribal_land_frac`|Fraction of county area under American Indian, Alaska Native, or Native Hawaiian governance.|Census||
|Environmental Justice|`total_tracts`|Number of Census tracts contained in this county|Justice40||
||`justice40_dbcp_index`|Proprietary environmental justice score. See Justice40 section below.|Justice40||
||`n_distinct_qualifying_tracts`|Number of distinct tracts that meet Justice40's criterion for "disadvantaged" within this county|Justice40||
||`n_tracts_agriculture_loss_low_income_not_students`|Number of tracts with high predicted climate-driven agriculture loss and low income (excepting students)|Justice40||
||`n_tracts_asthma_low_income_not_students`|Number of tracts with high asthma rates and low income (excepting students)|Justice40||
||`n_tracts_below_poverty_and_low_high_school`|Number of tracts with high poverty rates and low high school graduation rates|Justice40||
||`n_tracts_below_poverty_line_less_than_high_school_islands`|Number of tracts with high poverty rates and low high school graduation rates (island territories only)|Justice40||
||`n_tracts_building_loss_low_income_not_students`|Number of tracts with high predicted climate-driven building loss and low income (excepting students)|Justice40||
||`n_tracts_diabetes_low_income_not_students`|Number of tracts with high diabetes rates and low income (excepting students)|Justice40||
||`n_tracts_diesel_particulates_low_income_not_students`|Number of tracts with high diesel emissions and low income (excepting students)|Justice40||
||`n_tracts_energy_burden_low_income_not_students`|Number of tracts with high energy burden and low income (excepting students)|Justice40||
||`n_tracts_hazardous_waste_proximity_low_income_not_students`|Number of tracts with close proximity to hazardous waste sites and low income (excepting students)|Justice40||
||`n_tracts_heart_disease_low_income_not_students`|Number of tracts with high heart disease rates and low income (excepting students)|Justice40||
||`n_tracts_housing_burden_low_income_not_students`|Number of tracts with high housing burden and low income (excepting students)|Justice40||
||`n_tracts_lead_paint_and_median_home_price_low_income_not_studen`|Number of tracts with high lead paint exposure, high home prices, and low income (excepting students)|Justice40||
||`n_tracts_life_expectancy_low_income_not_students`|Number of tracts with low life expectancy and low income (excepting students)|Justice40||
||`n_tracts_linguistic_isolation_and_low_high_school`|Number of tracts with high linguistic isolation and low high school graduation rates|Justice40||
||`n_tracts_local_to_area_income_ratio_and_low_high_school`|Number of tracts with low ratios of local to regional income and low high school graduation rates|Justice40||
||`n_tracts_local_to_area_income_ratio_less_than_high_school_islan`|Number of tracts with low ratios of local to regional income and low high school graduation rates (island territories only)|Justice40||
||`n_tracts_pm2_5_low_income_not_students`|Number of tracts with high particulate matter pollution and low income (excepting students)|Justice40||
||`n_tracts_population_loss_low_income_not_students`|Number of tracts with high predicted climate-driven population loss and low income (excepting students)|Justice40||
||`n_tracts_risk_management_plan_proximity_low_income_not_students`|Number of tracts with close proximity to RMP sites and low income (excepting students)|Justice40||
||`n_tracts_superfund_proximity_low_income_not_students`|Number of tracts with close proximity to superfund sites and low income (excepting students)|Justice40||
||`n_tracts_traffic_low_income_not_students`|Number of tracts with high traffic exposure and low income (excepting students)|Justice40||
||`n_tracts_unemployment_and_low_high_school`|Number of tracts with high unemployment and low high school graduation rates|Justice40||
||`n_tracts_unemployment_less_than_high_school_islands`|Number of tracts with high unemployment and low high school graduation rates (island territories only)|Justice40||
||`n_tracts_wastewater_low_income_not_students`|Number of tracts with high wastewater pollution and low income (excepting students)|Justice40||
|Elections|`next_election_date`|The date of closest upcoming election in the county.|Ballot Ready||
||`next_election_reference_year`|Refers to a base year that the election frequency can be calculated from, either into the future or the past|Ballot Ready|For example, a position with a reference_year of 2022 and frequency of 4 has scheduled elections in 2018, 2022, 2026, etc.|
||`next_election_frequency`|How often the position is regularly scheduled for election|Ballot Ready|Certain positions are scheduled for election on non-standard frequences, such as 4 years then 2 years then 4 years (4-2-4) to align with 10 year redistricting cycles. Thus the field is returned as an array of frequncies rather than a single integer.|
||`next_election_position_name`|The name of the position that encompasses both official ballot and BallotReady position naming conventions|Ballot Ready||
||`next_election_number_of_seats`|The maximum number of people who will be elected to that position during a given race.|Ballot Ready|Note that staggered term positions will have seats on a board split over multiple positions. For example, a 5 member board that elects 2 members in 2022 and 3 members in 2024 will have two separate position records, one with 2 seats and one with 3 seats.|

## Modeling Decisions

With the exception of the two columns mentioned above, this is a restructured version of counties_long_format. See the entry for that table for description of methodology:

{% content-ref url="counties_long_format.md" %}
[counties\_long\_format.md](counties\_long\_format.md)
{% endcontent-ref %}

The following are in addition to, not instead of, those modeling decisions.

### Definition of "Renewables" includes Battery Storage

The columns prefixed with `renewables_` contain aggregates of both generation and storage assets. The generation sources are solar photovoltaic, onshore wind, and offshore wind techonologies. The only included storage type is battery storage. Batteries are included here because

* batteries increase capacity factors of wind & solar plants
* batteries allow arbitrage against tariffs, enabling higher project values
* batteries are also additional local economic development (more tax base)
