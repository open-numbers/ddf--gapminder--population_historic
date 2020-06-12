# coding: utf-8

import os.path as osp
import pandas as pd

from lib import run

DOCID = '14_suWY8fCPEXV0MH7ZQMZ-KndzMVsSsA5HdR-7WqAC0'
SHEET_COUNTRY = 'data-for-countries-etc-by-year'
SHEET_GLOBAL = 'data-for-world-by-year'
SHEET_REGION = 'data-for-regions-by-year'

OUT_DIR = '../../'

COLUMN_TO_CONCEPT = {'Population': 'population_total'}

DIMENSIONS = ['geo', 'country', 'world_4regions', 'global', 'time']
DIMENSION_TYPES = ['entity_domain', 'entity_set', 'entity_set', 'entity_set', 'time']
DIMENSION_DOMAINS = ['', 'geo', 'geo', 'geo', '']


def main():
    print('running etl...')

    country_measure, country_ent = run(DOCID, SHEET_COUNTRY, ['geo', 'time'],
                                       {'geo': 'country'}, COLUMN_TO_CONCEPT, OUT_DIR)
    region_measure, region_ent = run(DOCID, SHEET_REGION, ['geo', 'time'],
                                     {'geo': 'world_4regions'}, COLUMN_TO_CONCEPT, OUT_DIR)
    global_measure, global_ent = run(DOCID, SHEET_GLOBAL, ['geo', 'time'],
                                     {'geo': 'global'}, COLUMN_TO_CONCEPT, OUT_DIR)

    measures_df = pd.concat([country_measure, region_measure, global_measure],
                            ignore_index=True)

    dimensions_df = pd.DataFrame.from_dict(
        dict(concept=DIMENSIONS,
             name=list(map(str.title, DIMENSIONS)),
             concept_type=DIMENSION_TYPES,
             domain=DIMENSION_DOMAINS)
    )
    others_df = pd.DataFrame.from_dict(
        dict(concept=['name', 'domain'],
             name=['name', 'Domain'],
             concept_type=['string', 'string'])
    )
    (pd.concat([measures_df, dimensions_df, others_df], ignore_index=True, sort=True)
     .drop_duplicates()
     .to_csv(osp.join(OUT_DIR, 'ddf--concepts.csv'), index=False))

    country_ent['is--country'] = 'TRUE'
    country_ent.to_csv(osp.join(OUT_DIR, 'ddf--entities--geo--country.csv'), index=False)

    region_ent['is--world_4regions'] = 'TRUE'
    region_ent.to_csv(osp.join(OUT_DIR, 'ddf--entities--geo--world_4regions.csv'), index=False)

    global_ent['is--global'] = 'TRUE'
    global_ent.to_csv(osp.join(OUT_DIR, 'ddf--entities--geo--global.csv'), index=False)


if __name__ == '__main__':
    main()
    print('Done.')
