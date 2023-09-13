# county_commission_election_info

This table contains the upcoming general, primary and run off races for county commissioners.

Not all counties will have races.

## Column Descriptions

**Unique Key Column(s):** (`county_fips_id`)
|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Elections|`county_name`|County name|Census||
||`county_id_fips`|County FIPS ID|Ballot Ready||
||`next_general_election_id`|Unique identifier for the next general election for county commissioners in the county|||
||`next_general_election_name`|A descriptive name for the next general election for county commissioners|||
||`next_general_election_day`|The date of the next general election for county commissioners|||
||`next_general_total_n_seats`|Total number of people who will be elected to positions for the election|||
||`next_general_total_n_races`|Total number of positions in the election. This will always be less than or equal to `total_n_seats` because a position can have multiple seats.|||
||`next_general_all_race_names`|All position names up for election|||
||`next_general_frequency`|How often the position is regularly scheduled for election. This field describes a position, not an election so the mode value is selected in the aggregation.|||
||`next_general_reference_year`|Refers to a base year that the election frequency can be calculated from, either into the future or the past. This field describes a position, not an election so the maximum value is selected in the aggregation.|||
||`next_primary_election_id`|Unique identifier for the next primary election for county commissioners in the county|||
||`next_primary_election_name`|A descriptive name for the next primary election for county commissioners|||
||`next_primary_election_day`|The date of the next primary election for county commissioners|||
||`next_primary_total_n_seats`|Total number of people who will be elected to positions for the election|||
||`next_primary_total_n_races`|Total number of positions in the election. This will always be less than or equal to `total_n_seats` because a position can have multiple seats.|||
||`next_primary_all_race_names`|All position names up for election|||
||`next_primary_frequency`|How often the position is regularly scheduled for election. This field describes a position, not an election so the mode value is selected in the aggregation.|||
||`next_primary_reference_year`|Refers to a base year that the election frequency can be calculated from, either into the future or the past. This field describes a position, not an election so the maximum value is selected in the aggregation.|||
||`next_run_off_election_id`|Unique identifier for the next run off election for county commissioners in the county|||
||`next_run_off_election_name`|A descriptive name for the next run off election for county commissioners|||
||`next_run_off_election_day`|The date of the next run off election for county commissioners|||
||`next_run_off_total_n_seats`|Total number of people who will be elected to positions for the election|||
||`next_run_off_total_n_races`|Total number of positions in the election. This will always be less than or equal to `total_n_seats` because a position can have multiple seats.|||
||`next_run_off_all_race_names`|All position names up for election|||
||`next_run_off_frequency`|How often the position is regularly scheduled for election. This field describes a position, not an election so the mode value is selected in the aggregation.|||
||`next_run_off_reference_year`|Refers to a base year that the election frequency can be calculated from, either into the future or the past. This field describes a position, not an election so the maximum value is selected in the aggregation.|||
