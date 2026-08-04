# columbia_reldi_local_opposition__state_policy

This table contains state-level policy records from Columbia's Renewable Energy Legal Defense Initiative (RELDI) opposition-to-renewables source.

## Table Details

**Source:** Columbia RELDI local opposition

**Grain:** One row per state policy entry

**Primary key column(s):** `state_id_fips`

**Purpose:** State-level policy text with state FIPS and extracted year-summary fields.

## Transformations

The ETL adds state FIPS codes to the raw state policy records and renames the source state column to `raw_state_name`.

It also extracts year-like values from the policy text and summarizes them into `earliest_year_mentioned`, `latest_year_mentioned`, and `n_years_mentioned`. These fields are intended to summarize the source text, not to guarantee a formal enacted year.

## Related Tables

- `columbia_reldi_local_opposition__local_ordinance`
- `census__state_fips`
