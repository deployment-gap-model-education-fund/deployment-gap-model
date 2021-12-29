from typing import Dict, Any, List

import pandas as pd
import numpy as np
from pudl.metadata.enums import US_STATES_TERRITORIES


def transform(raw_df: pd.DataFrame) -> pd.DataFrame:
    transform_df = raw_df.copy()
    transform_df.loc[:, 'permitting_type'].replace('n/a', pd.NA, inplace=True)
    transform_df.loc[:, 'state'].replace(
        'Washington D.C.', 'District of Columbia', inplace=True)
    # manual error correction
    idx_MS = transform_df.index[(transform_df.loc[:, 'state']
                                 .eq('Mississippi'))
                                ][0]
    local_string = "Local governments have authority over land use and zoning decisions."
    if transform_df.at[idx_MS, 'description'].endswith(local_string):
        transform_df.at[idx_MS, 'permitting_type'] = 'Local'

    dtypes = {
        'state': pd.StringDtype(),
        'permitting_type': pd.CategoricalDtype(),
        'description': pd.StringDtype(),
        'link': pd.StringDtype(),
    }
    transform_df = transform_df.astype(dtypes, copy=False)
    validate(transform_df)
    return transform_df


def validate(ncsl_df: pd.DataFrame) -> None:
    expected_states = set(US_STATES_TERRITORIES.values())
    df_states = set(ncsl_df.loc[:, 'state'].unique())
    # don't want symmetric diff due to territories
    set_diff = df_states.difference(expected_states)
    if len(set_diff) > 0:
        raise AssertionError(f"Unexpected state: {set_diff}")

    # categorical dtype doesn't use pd.NA
    expected_types = {'State', 'Local', 'Hybrid', np.nan}
    df_types = set(ncsl_df.loc[:, 'permitting_type'].unique())
    set_diff = df_types.difference(expected_types)
    if len(set_diff) > 0:
        raise AssertionError(f"Unexpected permitting type: {set_diff}")
    return
