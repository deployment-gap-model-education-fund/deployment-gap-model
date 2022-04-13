"""Functions to transform EIP Infrastructure tables."""

import logging
from typing import Dict, List, Sequence

import pandas as pd

from dbcp.schemas import TABLE_SCHEMAS
from dbcp.transform.helpers import add_county_fips_with_backup_geocoding, replace_value_with_count_validation

logger = logging.getLogger(__name__)


class EIPTransformer(object):
    def __init__(self, raw_dfs: Dict[str, pd.DataFrame]) -> None:
        pass

    @staticmethod
    def _format_column_names(cols: Sequence[str]) -> List[str]:
        out = [(col.lower()
                .replace(' ', '_')
                .replace('(', '')
                .replace(')', ''))
               for col in cols]
        return out

    @staticmethod
    def facilities_transform(raw_fac_df: pd.DataFrame) -> pd.DataFrame:
        fac = raw_fac_df.copy()
        fac.columns = EIPTransformer._format_column_names(fac.columns)
        rename_dict = {  # add 'raw_' prefix to columns that need transformation
            'raw_id': 'facility_id',
            'state': 'raw_state',
            'county_or_parish': 'raw_county_or_parish',
            'location': 'raw_location',
            'modified_on': 'raw_modified_on',
        }
        fac.rename(columns=rename_dict, inplace=True)

        # fix 9 (as of 3/22/2022) states that are CSV duplicates like "LA, LA"
        fac['state'] = fac['raw_state'].str.split(',', n=1).str[0]

        # fix 4 counties (as of 3/22/2022) with multiple values as CSV.
        # Simplify by only taking first county
        fac['county'] = fac['raw_county_or_parish'].str.split(',', n=1).str[0]
        # standardize null values (only 2)
        fac['county'].replace('TDB', pd.NA, inplace=True)

        fac = add_county_fips_with_backup_geocoding(fac, state_col='state', locality_col='county')
        fac.drop(columns=['state', 'county'], inplace=True)  # drop intermediates

        coords = fac.loc[:, 'raw_location'].str.split(',', n=1, expand=True)
        for col in coords.columns:
            coords.loc[:, col] = pd.to_numeric(coords.loc[:, col], errors='raise')
        # check order is as assumed
        assert coords.iloc[:, 0].max() < 0  # USA longitudes
        assert coords.iloc[:, -1].min() > 0  # USA latitudes
        fac[['longitude', 'latitude']] = coords

        fac['date_modified'] = pd.to_datetime(fac.loc[:, 'raw_modified_on'], infer_datetime_format=True)

        duplicative_columns = [  # these are raw names
            # These columns are just a concatenation of the names and IDs corresponding to the ID columns
            # They add no information and invite inconsistency
            'Company',
            'Project',
            'Associated Facilities',
            'Pipelines',
            'Air Operating',
            'CWA-NPDES',
            'Other Permits',
        ]
        duplicative_columns = EIPTransformer._format_column_names(duplicative_columns)
        fac.drop(columns=duplicative_columns, inplace=True)

        return fac

    @staticmethod
    def _parse_id_cols_to_associative_entity_tables(id_df: pd.DataFrame, idx_col: str) -> Dict[str, pd.DataFrame]:
        ids = id_df.set_index(idx_col)  # copy
        out = {f"eip_assoc_{idx_col}_to_{col}": EIPTransformer._id_col_to_associative_entity_table(ids.loc[:, col])
               for col in ids.columns}
        return out

    @staticmethod
    def _id_col_to_associative_entity_table(id_ser: pd.Series) -> pd.DataFrame:
        assoc_table = id_ser.str.split(',', expand=True).stack().droplevel(1, axis=0)
        assoc_table = pd.to_numeric(assoc_table, downcast='unsigned')
        return assoc_table

    @staticmethod
    def projects_transform(raw_proj_df: pd.DataFrame) -> pd.DataFrame:
        proj = raw_proj_df.copy()
        proj.columns = EIPTransformer._format_column_names(proj.columns)
        rename_dict = {  # add 'raw_' prefix to columns that need transformation
            'sulfur_dioxide_so2': 'raw_sulfur_dioxide_so2',
            'project_cost_million_$': 'raw_project_cost_million_$',
            'modified_on': 'raw_modified_on',
            'created_on': 'raw_created_on',
            'facility_id': 'raw_facility_id',
            'industry_sector': 'raw_industry_sector',
            'project_type': 'raw_project_type',
            'construction_status_last_updated': 'raw_construction_status_last_updated',
            'actual_or_expected_completion_year': 'raw_actual_or_expected_completion_year',
            'number_of_jobs_promised': 'raw_number_of_jobs_promised',
            'operating_status': 'raw_operating_status',
            'id': 'project_id',
            'product_type_private,_as': 'product_type',
        }
        proj.rename(columns=rename_dict, inplace=True)

        # transform columns
        proj['so2_tons_per_year'] = EIPTransformer._fix_erroneous_array_items(proj.loc[:, 'raw_sulfur_dioxide_so2'])
        proj['cost_millions'] = EIPTransformer._fix_erroneous_array_items(proj.loc[:, 'raw_project_cost_million_$'])
        proj['date_modified'] = pd.to_datetime(proj.loc[:, 'raw_modified_on'], infer_datetime_format=True)
        proj['operating_status'] = proj.loc[:, 'raw_operating_status'].copy()
        replace_value_with_count_validation(  # in place
            df=proj,
            col='operating_status',
            val_to_replace="Unknown",
            replacement=pd.NA,
            expected_count=1
        )
        proj['industry_sector'] = proj.loc[:, 'raw_industry_sector'].copy()
        replace_value_with_count_validation(  # in place
            df=proj,
            col='industry_sector',
            val_to_replace="Petrochemicals and Plastics, Other",
            replacement="Petrochemicals and Plastics",
            expected_count=1
        )

        duplicative_columns = [  # these are raw names
            # These columns are just a concatenation of the names and IDs corresponding to the ID columns
            # They add no information but invite inconsistency
            'Facility',
            'Air Construction',
            'Air Operating',
            'NGA',
            'MARAD',
            'Other Permits',
        ]
        duplicative_columns = EIPTransformer._format_column_names(duplicative_columns)
        proj.drop(columns=duplicative_columns, inplace=True)

        return proj

    @staticmethod
    def _fix_erroneous_array_items(ser: pd.Series) -> pd.Series:
        """Split on commas, preserve only the first value, and cast to numeric.

        Several columns in EIP data should be numeric types but a small number of erroneous
        values forces them to object dtype. The erroneous pattern is for the number to simply
        be duplicated as a CSV string. For example, 0.2 appears as '0.2, 0.2'.

        Args:
            ser (pd.Series): values to fix

        Returns:
            pd.Series: fixed series
        """
        first_values = ser.str.split(',', n=1).str[0]
        out = pd.to_numeric(first_values, errors='raise')
        return out

    @staticmethod
    def air_constr_transform(raw_air_constr_df: pd.DataFrame) -> pd.DataFrame:
        air = raw_air_constr_df.copy()
        air.columns = EIPTransformer._format_column_names(air.columns)
        rename_dict = {  # add 'raw_' prefix to columns that need transformation
            'id': 'air_constr_id',
            'created_on': 'raw_created_on',
            'modified_on': 'raw_modified_on',
            'date_last_checked': 'raw_date_last_checked',
            'project_id': 'raw_project_id',
            'statute': 'raw_statute',
            'permit_type': 'raw_permit_type',
            'permitting_action': 'raw_permitting_action',
            'permit_status': 'raw_permit_status',
            'application_date': 'raw_application_date',
            'draft_permit_issuance_date': 'raw_draft_permit_issuance_date',
            'last_day_to_comment': 'raw_last_day_to_comment',
            'final_permit_issuance_date': 'raw_final_permit_issuance_date',
            'deadline_to_begin_construction': 'raw_deadline_to_begin_construction',
        }
        air.rename(columns=rename_dict, inplace=True)

        # transform columns
        air['date_modified'] = pd.to_datetime(air.loc[:, 'raw_modified_on'], infer_datetime_format=True)
        air['permit_status'] = air.loc[:, 'raw_permit_status'].copy()
        replace_value_with_count_validation(  # in place
            df=air,
            col='permit_status',
            val_to_replace="Withdrawn (UARG v. EPA 134 S. Ct. 2427 (2014))",
            replacement='Withdrawn',
            expected_count=12
        )

        duplicative_columns = [  # these are raw names
            # These columns are just a concatenation of the names and IDs corresponding to the ID columns
            # They add no information but invite inconsistency
            'Project',
        ]
        duplicative_columns = EIPTransformer._format_column_names(duplicative_columns)
        air.drop(columns=duplicative_columns, inplace=True)

        return air
