# iso_counties_change_log

Each row in the table is a monthly snap shot of projects that enter, are withdrawn and become operational queues for each county in the continental US.

## Column Descriptions

| Subject | Column                            | Description                                 | Source | Notes |
|---------|-----------------------------------|---------------------------------------------|--------|-------|
|         | `iso_region`                      |ISO Region                                   | GS |       |
|         | `date`                            |Date of snapshot (monthly)                   | GS |       |
|         | `withdrawn_clean_n_projects`      |Number of clean projects that left the queue | GS |       |
|         | `suspended_clean_n_projects`      |Number of clean projects that became suspended | GS |       |
|         | `new_clean_n_projects`            |Number of clean projects that enetered the queue | GS |       |
|         | `operational_clean_n_projects`    |Number of clean  projects that became operational | GS |       |
|         | `withdrawn_fossil_n_projects`     |Number of fossil projects that left the queue | GS |       |
|         | `suspended_fossil_n_projects`     |Number of fossil projects that became suspended | GS |       |
|         | `operational_fossil_n_projects`   |Number of fossil  projects that became operational | GS |       |
|         | `new_fossil_n_projects`           |Number of fossil projects that enetered the queue | GS |       |
|         | `new_other_n_projects`            |Number of other projects that enetered the queue | GS |       |
|         | `withdrawn_other_n_projects`      |Number of other projects that left the queue | GS |       |
|         | `suspended_other_n_projects`      |Number of other projects that became suspended | GS |       |
|         | `operational_other_n_projects`    |Number of other  projects that became operational | GS |       |
|         | `withdrawn_clean_capacity_mw`     |Total clean capacity (MW) that left the queue | GS |       |
|         | `suspended_clean_capacity_mw`     |Total clean capacity (MW) that became suspended | GS |       |
|         | `new_clean_capacity_mw`           |Total clean capacity (MW) that enetered the queue | GS |       |
|         | `operational_clean_capacity_mw`   |Total clean capacity (MW) that became operational | GS |       |
|         | `withdrawn_fossil_capacity_mw`    |Total fossil capacity (MW) that left the queue | GS |       |
|         | `suspended_fossil_capacity_mw`    |Total fossil capacity (MW) that became suspended | GS |       |
|         | `operational_fossil_capacity_mw`  |Total fossil capacity (MW) that became operational | GS |       |
|         | `new_fossil_capacity_mw`          |Total fossil capacity (MW) that enetered the queue | GS |       |
|         | `new_other_capacity_mw`           |Total other capacity (MW) that enetered the queue | GS |       |
|         | `withdrawn_other_capacity_mw`     |Total other capacity (MW) that left the queue | GS |       |
|         | `suspended_other_capacity_mw`     |Total other capacity (MW) that became suspended | GS |       |
|         | `operational_other_capacity_mw`   |Total other capacity (MW) that became operational | GS |       |
