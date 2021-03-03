# coding=utf-8

import logging

from json import load
from glob import glob
from pandas import read_csv
from pandas import to_datetime
from datetime import datetime as dt

from helpers.get_donauts_from_sat import *


logger = logging.getLogger('donauts_sat')
logger.setLevel(logging.INFO)
logger.propagate = False

formatter = logging.Formatter("%(asctime)s [%(name)s] - %(message)s")

fh = logging.FileHandler('./logs/donaut_sat.log')
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

logger.addHandler(fh)

def get_donauts(
    year=2020
    , from_sat = False
    ):
    logger.info('Getting Donatarias Autorizadas directory')
    logger.info('Checking if directory already exists')
    
    if from_sat:
        n_files = 0
    else:
        try:
            files = glob('../resources/data/donaut-sat/csv/*.csv')
            n_files = len(files)
        except Exception as e:
            logger.error('sirfosc directory not found')
    
    if n_files == 1: 
        logger.info('One directory found, loading data from it')
        file = str(files[0])
    
    elif n_files > 1: 
        logger.info('Multiple directories found, loading data from most recent')
        dts = [to_datetime(dt[-23:-4] \
            , format='%Y-%m-%d-%H-%M-%S') for dt in files]
        recent_file = max(dts)
        file = (
            f'../resources/data/donaut-sat/csv/report-sat-{str(year)}-' 
            + str(recent_file).replace(' ', '-').replace(':', '-') 
            + '.csv'
            )
    
    else:
        logger.info('No Donatarias Autorizadas directories found, downloading from SAT')
        
        try:
            df, now = get_donauts_from_sat(
                year
                , '../resources/data/donaut-sat/sheets/'
                )
            file = f'../resources/data/donaut-sat/csv/report-sat-{str(year)}-{now}.csv'
            df.to_csv(file)
            logger.info(f'Data loaded from response, saved at: {file}')
        except Exception as e:
            logger.error('something occurred when talking to sat server')
            raise(e)

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

    df_donaut_sat['fecha_oficio_sat'] = to_datetime(                            # TODO: formating and data cleaning for entire dataset
        df_donaut_sat.fecha_oficio_sat
        )

    df_donaut_sat.loc[:, 'sat_year'] = str(year)

    logger.info('Donatarias Autorizadas direectory created\n')
    
    return(df)
