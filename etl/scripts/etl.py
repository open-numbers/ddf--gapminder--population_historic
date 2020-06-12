# coding: utf-8

import os
import os.path as osp
import glob
import shutil
import pandas as pd

from lib import run

DOCID = '14_suWY8fCPEXV0MH7ZQMZ-KndzMVsSsA5HdR-7WqAC0'
SHEET_COUNTRY = 'data-for-countries-etc-by-year'
SHEET_GLOBAL = 'data-for-world-by-year'
SHEET_REGION = 'data-for-regions-by-year'

OUT_DIR = '../../'

COLUMN_TO_CONCEPT = {'Population': 'population_total'}


def main():
    try:
        datasets_dir = os.environ['DATASETS_DIR']
    except KeyError:
        datasets_dir = '../../../../'

    print('running etl...')

    country_measure, country_ent = run(DOCID, SHEET_COUNTRY, ['geo', 'time'],
                                       None, COLUMN_TO_CONCEPT, OUT_DIR)
    region_measure, region_ent = run(DOCID, SHEET_REGION, ['geo', 'time'],
                                     {'geo': 'world_4region'},
                                     COLUMN_TO_CONCEPT, OUT_DIR)
    global_measure, global_ent = run(DOCID, SHEET_GLOBAL, ['geo', 'time'],
                                     {'geo': 'global'}, COLUMN_TO_CONCEPT,
                                     OUT_DIR)

    measures_df = pd.concat([country_measure, region_measure, global_measure],
                            ignore_index=True)

    open_numbers_df = pd.read_csv(
        osp.join(datasets_dir, 'open-numbers/ddf--open_numbers/',
                 'ddf--concepts.csv'))

    (pd.concat([measures_df, open_numbers_df], ignore_index=True,
               sort=True).drop_duplicates().to_csv(osp.join(
                   OUT_DIR, 'ddf--concepts.csv'),
                                                   index=False))

    for ent_file in glob.glob(
            osp.join(datasets_dir, 'open-numbers/ddf--open_numbers/',
                     'ddf--entities--geo--*.csv')):
        shutil.copy(ent_file, OUT_DIR)


if __name__ == '__main__':
    main()
    print('Done.')
