# county_commission_election_info

This table contains all upcoming county commissioner elections from Ballot Ready.

## Column Descriptions

**Unique Key Column(s):** (`election_id`, `county_fips_id`)
|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Elections|`county_name`|County name|Census||
||`county_id_fips`|County FIPS ID|Ballot Ready||
||`election_id`|A unique identifier for each election in the BallotReady database|||
||`election_name`|A descriptive name for the election according to BallotReady's naming conventions.|||
||`election_day`|The date of the election|||
||`total_n_of_seats`|Total number of people who will be elected to positions for the election.|||
||`total_n_races`|Total number of positions in the election. This will always be less than or equal to `total_n_of_seats` because a position can have multiple seats.|||
||`all_race_names`|All position names up for election.|||
||`frequency`|How often the position is regularly scheduled for election. This field describes a position, not an election so the maximum value is selected in the aggregation.|||
||`reference_year`|Refers to a base year that the election frequency can be calculated from, either into the future or the past. This field describes a position, not an election so the maximum value is selected in the aggregation.|||
