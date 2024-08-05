"""Create county-level aggregates of proposed projects from EIA860m and ACP data."""
from typing import Optional

import pandas as pd
import sqlalchemy as sa

from dbcp.helpers import get_sql_engine


def _get_concrete_aggs(engine: sa.engine.Engine) -> pd.DataFrame:
    query = """
    WITH
    eia860m as (
        SELECT DISTINCT ON (plant_id_eia, generator_id)
            plant_id_eia,
            generator_id,
            capacity_mw,
            CASE -- need to pick 'MWH' out of the 'other' category
                WHEN energy_source_code_1 = 'MWH' THEN 'storage'
                ELSE fuel_type_code_pudl
            END AS resource_clean,
            CASE -- map to ACP categories
                WHEN operational_status_code <= 3 THEN 'Advanced Development'
                WHEN operational_status_code > 3 THEN 'Under Construction'
            END AS status,
            county_id_fips,
            NULL as iso_rto
        FROM data_warehouse.pudl_eia860m_changelog
        where operational_status_code BETWEEN 1 AND 6
        ORDER BY plant_id_eia, generator_id, valid_until_date desc
    ),
    acp AS (
        SELECT
            plant_id_eia,
            NULL as generator_id,
            capacity_mw,
            CASE -- 860m doesn't separate offshore from onshore wind
                WHEN resource = 'Offshore Wind' THEN 'wind'
                ELSE lower(resource)
            END AS resource_clean,
            status,
            county_id_fips,
            iso_region
        from private_data_warehouse.acp_projects as acp
        where status in ('Advanced Development', 'Under Construction')
            AND NOT EXISTS (
                SELECT 1
                FROM eia860m
                WHERE eia860m.plant_id_eia = acp.plant_id_eia
            )
    ),
    all_projects AS (
        SELECT * FROM eia860m
        UNION ALL
        SELECT * FROM acp
    ),
    aggs as (
        SELECT
            county_id_fips,
            resource_clean,
            -- iso_region,  I don't have this for 860m yet
            -- pivot the statuses into columns per client request
            SUM(
                CASE
                    WHEN status = 'Advanced Development' THEN capacity_mw
                    ELSE 0.0
                END
            ) as capacity_awaiting_permitting_mw,
            SUM(
                CASE
                    WHEN status = 'Under Construction' THEN capacity_mw
                    ELSE 0.0
                END
            ) as capacity_under_construction_mw,
            sum(capacity_mw) as capacity_total_proposed_mw
        FROM all_projects
        GROUP BY 1, 2 --, 3
    )
    select
        sfips.state_name as state,
        cfips.county_name as county,
        substr(aggs.county_id_fips, 1, 2) as state_id_fips,
        aggs.*,
        NULL as iso_region
    from aggs
    LEFT JOIN data_warehouse.county_fips AS cfips
    ON aggs.county_id_fips = cfips.county_id_fips
    LEFT JOIN data_warehouse.state_fips AS sfips
    ON substr(aggs.county_id_fips, 1, 2) = sfips.state_id_fips
    order by state, county, resource_clean
    ;
    """
    df = pd.read_sql(query, engine)
    return df


def create_data_mart(
    engine: Optional[sa.engine.Engine] = None,
) -> pd.DataFrame:
    """API function to create the table of project aggregates.

    Args:
        engine (Optional[sa.engine.Engine], optional): database connection. Defaults to None.

    Returns:
        pd.DataFrame: Dataframe of EIA860m and ACP projects.
    """
    if engine is None:
        engine = get_sql_engine()
    df = _get_concrete_aggs(engine=engine)
    return df
