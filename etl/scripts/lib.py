"""helper functions
"""

import os.path as osp
import pandas as pd

from ddf_utils.io import open_google_spreadsheet, serve_datapoint


def get_new_dimension(dimensions, column_entity_set_map):
    new_dims = list()
    for d in dimensions:
        if d in column_entity_set_map:
            new_dims.append(column_entity_set_map[d])
        else:
            new_dims.append(d)

    return new_dims


def run(docid, sheet, dimensions, column_entity_set_map, column_concept_map,
        out_dir):
    print(f"reading sheet: {sheet}")
    data = pd.read_excel(open_google_spreadsheet(docid), sheet_name=sheet, dtype={'time': int})

    measures = list()

    if column_entity_set_map:
        new_dims = get_new_dimension(dimensions, column_entity_set_map)
        df = data.rename(columns=column_entity_set_map)
        entities_df = data[[
            'geo', 'name'
        ]].drop_duplicates().rename(columns=column_entity_set_map)
    else:
        new_dims = dimensions
        df = data
        entities_df = data[['geo', 'name']].drop_duplicates()

    df_dps = df.set_index(new_dims).drop('name', axis=1)
    for c in df_dps:
        df_dp = df_dps[[c]]
        c_id = column_concept_map[c]
        df_dp.columns = [c_id]
        serve_datapoint(df_dp, out_dir, c_id)

        measures.append((c_id, c))

    measures_df = pd.DataFrame(measures, columns=['concept', 'name'])
    measures_df['concept_type'] = 'measure'

    return measures_df, entities_df
