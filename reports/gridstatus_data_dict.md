# Data Dictionary

| Column                      | Non-Null Count  | Dtype         | Description |
|---                          | --------------  | -----         | ----------- |
| `project_id`                  | 7169 non-null   | Int64         | Internal, generated ID. Unstable, so probably not useful for comparison.            |
| `actual_completion_date`      | 2 non-null      | datetime64[ns]|             |
| `capacity_mw`                 | 7149 non-null   | float64       |             |
| `county`                      | 7063 non-null   | string        |             |
| `resource`                    | 7062 non-null   | string        | Fuel type. Can be ambiguous for X + Storage projects. Note that some are transmission projects; not sure if all ISOs include those.           |
| `interconnecting_entity`      | 1053 non-null   | string        |             |
| `point_of_interconnection`    | 3878 non-null   | string        | Usually the name of some substation            |
| `project_name`                | 5233 non-null   | string        |             |
| `proposed_completion_date`    | 3074 non-null   | datetime64[ns]|             |
| `queue_date`                  | 7158 non-null   | datetime64[ns]| Date the project entered the queue (was proposed).            |
| `queue_id`                    | 7169 non-null   | string        | ID from the ISOs themselves. I think it's unique in combination with `iso_region` but needs checking.            |
| `state`                       | 7135 non-null   | string        |             |
| `queue_status`                | 7169 non-null   | string        | I believe these have been filtered to only "active" status.            |
| `summer_capacity_mw`          | 5590 non-null   | float64       |             |
| `transmission_owner`          | 5842 non-null   | string        |             |
| `winter_capacity_mw`          | 5609 non-null   | float64       |             |
| `withdrawal_comment`          | 0 non-null      | string        |             |
| `withdrawn_date`              | 9 non-null      | datetime64[ns]|             |
| `is_actionable`               | 7169 non-null   | boolean       | Client's custom status indicator. This means the project is under some stage of review.            |
| `is_nearly_certain`           | 7169 non-null   | boolean       | Client's custom status indicator. This means the project has been approved and is very likely to be built.            |
| `iso_region`                  | 7169 non-null   | string        | Which ISO the project is in.            |
| `state_id_fips`               | 7130 non-null   | string        | State FIPS            |
| `county_id_fips`              | 7043 non-null   | string        | County FIPS via Google            |
| `geocoded_locality_name`      | 7169 non-null   | string        | The name Google thinks is associated with the `county` entry.            |
| `geocoded_locality_type`      | 7169 non-null   | string        | The jurisdition type Google thinks is associated with the `county` entry.            |
| `geocoded_containing_county`  | 7053 non-null   | string        | If a town/city, what county is it in.            |
| `resource_clean`              | 7169 non-null   | string        | A standardized version of `resource`            |
