# br_election_data

This table contains a denormalized view of election data from Ballot Ready where each row is a unique race.

This table contains a few entities that are worth describing:

- `position` is the office/position someone is running for (president, Alaska House of Reps District 1). There can be multiple elections for a single position (special election, runoffs, general...)
- `election` is the election/event that elects people to positions (special election, runoffs, general...). There can be multiple positions for an election. For example,the 2024 Georgia General Election has ~1500 positions.
- `race`` is a unique combination of a position and an election. It is unique in this dataset.

## Column Descriptions

**Unique Key Column(s):** `race_id`
|Subject|Column|Description|Source|Notes|
|----|----|----|----|----|
|Elections|`county`|County name||Ballot Ready|
||`election_id`|A unique identifier for each election in the BallotReady database|||
||`election_name`|A descriptive name for the election according to BallotReady's naming conventions.description|||
||`election_day`|The date of the election|||
||`race_id`|A unique identifier for each instance of a position and election combination in the BallotReady database|||
||`is_primary`|A boolean marked as true if the race is primary|||
||`is_runoff`|A boolean marked as true if the race is a runoff|||
||`is_unexpired`|A boolean marked as true if the race is to fill an unexpired term |||
||`position_id`|A unique identifier for each position in the BallotReady database|||
||`position_name`|The name of the position that encompasses both official ballot and BallotReady position naming conventions|||
||`sub_area_name`|A parsed portion of the position name, used to help identify the specific area the position represents. Sub_area_name is used to identify the type of sub area (e.g., "District" or "Region").|||
||`sub_area_value`|A parsed portion of the position name, used to help identify the specific area the position represents. Sub_area_value is used to identify the number or office of the subarea (e.g., "1" or "West").|||
||`sub_area_name_secondary`|A second parsed portion of the position name, used to help identify the specific area the position represents when there are two levels of sub area.|||
||`sub_area_value_secondary`|A second parsed portion of the position name, used to help identify the specific area the position represents when there are two levels of sub area.|||
||`state`|The state in which the position is elected|||
||`level`|An identifier for the level of the position: federal, state, regional, county, city, or local|||
||`tier`|BallotReady-defined standard tiers (1-5)|||
||`is_judicial`|A boolean marked as true if the position is for electing a judge|||
||`is_retention`|A boolean that is true if the position is for a retention election (i.e., a yes/no vote)|||
||`number_of_seats`|The maximum number of people who will be elected to that position during a given race.|||
||`normalized_position_id`|A unique identifier for each generic position type (e.g., County Clerk, City Legislative, etc.). |||
||`normalized_position_name`|Describes the generic position type (e.g., County Clerk, City Legislative, etc.). |||
||`frequency`|How often the position is regularly scheduled for election|||
||`reference_year`|Refers to a base year that the election frequency can be calculated from, either into the future or the past|||
||`partisan_type`|A description for the partisan nature of the election of the position (either partisan, nonpartisan, or partisan for primary only). This pertains to whether the candidate will have a party affiliation listed on the ballot for their election.|||
||`race_created_at`|A timestamp of when the race's record was created, in UTC|||
||`race_updated_at`|A timestamp of when the race's record was most recently updated, in UTC|||
||`state_id_fips`|State FIPS ID|||
||`county_id_fips`|County FIPS ID|||
