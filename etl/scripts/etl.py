# coding: utf-8

import os.path as osp
import pandas as pd

from ddf_utils.io import open_google_spreadsheet, serve_datapoint


DOCID = '14_suWY8fCPEXV0MH7ZQMZ-KndzMVsSsA5HdR-7WqAC0'
SHEET = 'data-for-countries-etc-by-year'

DIMENSIONS = ['geo', 'time']
OUT_DIR = '../../'

COLUMN_TO_CONCEPT = {'Population': 'population_total'}


def gen_datapoints(df_: pd.DataFrame):
    df = df_.copy()
    df = df.set_index(DIMENSIONS).drop('name', axis=1)  # set index and drop column 'name'
    for c in df:
        yield (c, df[[c]])


def create_geo_domain(df: pd.DataFrame) -> pd.DataFrame:
    return df[['geo', 'name']].drop_duplicates()


def main():
    print('running etl...')
    data = pd.read_excel(open_google_spreadsheet(DOCID), sheet_name=SHEET)

    measures = list()

    for c, df in gen_datapoints(data):
        c_id = COLUMN_TO_CONCEPT[c]
        df.columns = [c_id]
        serve_datapoint(df, OUT_DIR, c_id)

        measures.append((c_id, c))

    measures_df = pd.DataFrame(measures, columns=['concept', 'name'])
    measures_df['concept_type'] = 'measure'

    dimensions_df = pd.DataFrame.from_dict(
        dict(concept=DIMENSIONS,
             name=list(map(str.title, DIMENSIONS)),
             concept_type=['entity_domain', 'time'])
    )
    others_df = pd.DataFrame.from_dict(
        dict(concept=['name'],
             name=['name'],
             concept_type=['string'])
    )
    (pd.concat([measures_df, dimensions_df, others_df], ignore_index=True)
     .to_csv(osp.join(OUT_DIR, 'ddf--concepts.csv'), index=False))

    geo_df = create_geo_domain(data)
    geo_df.to_csv(osp.join(OUT_DIR, 'ddf--entities--geo.csv'), index=False)


if __name__ == '__main__':
    main()
    print('Done.')
