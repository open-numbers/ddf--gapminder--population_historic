# -*- coding: utf-8 -*-

import pandas as pd
import os
from ddf_utils.str import to_concept_id


# configuration file paths
source = '../source/gapdata003 version 3.xlsx'
out_dir = '../../'

if __name__ == '__main__':
    data001 = pd.read_excel(source, sheet_name='Data')


    # entities
    area = data001['Area'].unique()
    area_id = list(map(to_concept_id, area))
    ent = pd.DataFrame([], columns=['area', 'name'])
    ent['area'] = area_id
    ent['name'] = area

    path = os.path.join(out_dir, 'ddf--entities--area.csv')
    ent.to_csv(path, index=False)

    # datapoints
    dp_name1 = 'Population'
    dp_name2 = 'Population with interpolations'
    data001_dp_1 = data001[['Area', 'Year', dp_name1]].copy()
    data001_dp_2 = data001[['Area', 'Year', dp_name2]].copy()

    data001_dp_1.columns = ['area', 'year', 'total_population']
    data001_dp_2.columns = ['area', 'year', 'total_population_with_interpolations']

    data001_dp_1['area'] = data001_dp_1['area'].map(to_concept_id)
    data001_dp_2['area'] = data001_dp_2['area'].map(to_concept_id)

    path1 = os.path.join(out_dir, 'ddf--datapoints--total_population--by--area--year.csv')
    path2 = os.path.join(out_dir, 'ddf--datapoints--total_population_with_interpolations--by--area--year.csv')

    data001_dp_1.dropna().sort_values(by=['area', 'year']).to_csv(path1, index=False)
    data001_dp_2.dropna().sort_values(by=['area', 'year']).to_csv(path2, index=False)

    # concepts
    conc = ['total_population', 'total_population_with_interpolations', 'area', 'year', 'name']
    cdf = pd.DataFrame([], columns=['concept', 'name', 'concept_type'])
    cdf['concept'] = conc
    cdf['name'] = ['Total Population', 'Total Population with interpolations', 'Area', 'Year', 'Name']
    cdf['concept_type'] = ['measure', 'measure', 'entity_domain', 'time', 'string']

    path = os.path.join(out_dir, 'ddf--concepts.csv')
    cdf.to_csv(path, index=False)

    print('Done.')
