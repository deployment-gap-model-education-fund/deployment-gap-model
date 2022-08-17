This table exists to support a Tableau dashboard. It is not intended to be used for other analysis. Instead, use one of the counties, iso_projects, or fossil_intrastructure tables from which this table was derived.

## Modeling Decisions
Because this table was derived from the counties, iso_projects, and fossil_intrastructure tables, all modeling decisions in those tables apply to this one. Additional modeling is described below.
### Project - Facility Relationships
In reality these items can have complex many-to-many relationships (multiple projects can occur at a given facility, and multiple facilities can be affected by a given project), but this complexity is unusual. Most are one-to-one.
In this table, the relationship between projects and facilities was simplified to avoid double counting. Projects were assigned to only the first associated facility, then aggregated by facility. Only 5 records were affected by this simplification.
