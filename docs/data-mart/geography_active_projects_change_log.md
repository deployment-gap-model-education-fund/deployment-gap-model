# Active project change logs
There are four tables where each row is the total capacity or number of projets in the queue for a given quarter and geography:
- `counties_active_projects_capacity_mw_change_log`,
- `iso_regions_active_projects_capacity_mw_change_log`,
- `counties_active_projects_n_projects_change_log`,
- `iso_regions_active_projects_n_projects_change_log`

Use these tables when you want to get a sense for what the queues looked like in previous years. Use `iso_regions_all_projects_change_log` and `counties_all_projects_change_log` when you want to understand the movement of projects in and out of the queue over time.

## Column Descriptions

| Subject | Column                            | Description                                 | Source | Notes |
|---------|-----------------------------------|---------------------------------------------|--------|-------|
|         | `{geography}`                     |Geography (iso_region or counties)           | GS |       |
|         | `report_date`                     |Date of snapshot (quarterly)                 | GS |       |
|         | `clean_{metric}`                  |Measure of clean projects (capacity_mw or n_projects) | GS |       |
|         | `fossil_{metric}`                 |Measure of fossil projects (capacity_mw or n_projects) | GS |       |
|         | `other_{metric}`                  |Measure of non fossil or clean projects (capacity_mw or n_projects) | GS |       |
