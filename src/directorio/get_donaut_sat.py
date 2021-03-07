# coding=utf-8

import logging

from pandas import read_csv
from pandas import to_datetime
from datetime import datetime as dt

from helpers.helpers import *


logger = logging.getLogger('donauts_sat')
logger.setLevel(logging.INFO)
logger.propagate = False

formatter = logging.Formatter("%(asctime)s [%(name)s] - %(message)s")

fh = logging.FileHandler('./logs/donaut_sat.log')
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

logger.addHandler(fh)

def get_donauts(year=2020, from_sat = False):
    
    logger.info('Getting Donatarias Autorizadas directory')
    
    files = check_sources(
        from_sat
        , f'../resources/data/donaut-sat/csv/{year}/' 
        , '.csv'
        , logger
        )
    
    file = get_source(
        'donauts-sat'
        , files
        , f'../resources/data/donaut-sat/{year}/'
        , '.csv'
        , logger
        )

    df = read_csv(file, low_memory=False).iloc[:, 1:]                           # drop index from csv file
    logger.info(f'Data loaded from directory: {file}')

    logger.info('Applying filters, cleaning and formatting')
    df_donaut_sat = (                                                           # drop duplicates for RFC and renaming
        df
        .drop_duplicates('RFC')
        [['RFC', 'FECHA DE OFICIO']]
        .rename(columns={
            'RFC': 'rfc'
            , 'FECHA DE OFICIO': 'fecha_oficio_sat'
        })
    )

    df_donaut_sat = (                                                           # removing osc with bad formatted rfc
        df_donaut_sat
        [df_donaut_sat.rfc.str.len().isin([12, 13])
        ].reset_index(drop=True)
        )

    df_donaut_sat['fecha_oficio_sat'] = to_datetime(                            # TODO: formating and data cleaning for entire dataset
        df_donaut_sat.fecha_oficio_sat
        )

    df_donaut_sat.loc[:, 'sat_year'] = str(year)

    logger.info('Donatarias Autorizadas direectory created\n')
    
    return(df)
