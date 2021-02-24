# coding=utf-8

import logging

from json import load
from glob import glob
from pandas import read_csv
from pandas import to_datetime
from datetime import datetime as dt

from helpers.get_clunis_from_sirfosc import *


logger = logging.getLogger('cluni')
logger.setLevel(logging.INFO)
logger.propagate = False

formatter = logging.Formatter("%(asctime)s [%(name)s] - %(message)s")

fh = logging.FileHandler('./logs/cluni.log')
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

logger.addHandler(fh)

def get_clunis(
    columns=None
    , from_sirfosc = False
    , status=['ACTIVA', 'INACTIVA']
    , representation=['VIGENTE', 'VENCIDA']
    ):
    
    logger.info('Getting CLUNI directory')
    logger.info('Checking if directory already exists')
    
    if from_sirfosc:
        n_files = 0
    else:
        try:
            files = glob('../resources/data/sirfosc/csv/*.csv')
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
            '../resources/data/sirfosc/csv/report-rfosc-' 
            + str(recent_file).replace(' ', '-').replace(':', '-') 
            + '.csv'
            )
    
    else:
        logger.info(
            'No CLUNI directories found, downloading from SIRFOSC'
            , exc_info=False
            )

        try:
            df, now = get_clunis_from_sirfosc('../resources/data/sirfosc/txt/')
        except Exception as e:
            logger.error('something occurred when talking to sirfosc server')
        
        file = f'../resources/data/sirfosc/csv/report-rfosc-{now}.csv'
        
        try:
            df.to_csv(file)
        except Exception as e:
            logger.error('directory creation (csv) failed')
        
        logger.info(
            f'Data loaded from response, saved at: {file}'
            )

        
    df = read_csv(file, low_memory=False).iloc[:, 1:]                           # drop index from csv file
    logger.info(f'Data loaded from directory: {file}')

    
    logger.info('Applying filtering, cleaning and formatting')
    df_cluni = df[
        df['ESTATUS'].isin(status)                                              # filter osc by cluni status
        & df['ESTATUS DE LA REPRESENTACION'].isin(representation)               # filter osc by rfosc representation
        ]
    df_cluni = df_cluni[df_cluni.RFC.str.len().isin([12, 13])]                  # removing osc with bad formatted rfc
    df_cluni = df_cluni.reset_index(drop=True)                                  # reset index due to removing entries
    
    if columns is None:                                                         # cleaning and formatting
        with open("./format/df_cluni.json", "r") as f: c = load(f)

    col_names = c['cols'].keys()
    new_col_names = [(k, v['name']) for k, v in c['cols'].items()]
    new_col_types = [(k, v['type']) for k, v in c['cols'].items() \
        if v['type'] in ['str', 'int']]
    
    df_cluni.astype(new_col_types) 
    
    for k, v in c['cols'].items():
        if v['type'] == 'date': df_cluni[k] = to_datetime(df_cluni[k])          # todo: there's better way to this with multiple columns?

    
    cols_to_lower = [k for k, v in c['cols'].items() if v['lower']]             # to lower values from certain columns
    df_cluni[cols_to_lower] = \
        df_cluni[cols_to_lower].apply(lambda c: c.str.lower())

    logger.info('Renaming columns and preparing dataset')
    df_cluni = df_cluni[col_names].rename(columns=dict(new_col_names))          # rename columns
    
    logger.info('CLUNI direectory created\n')

    return(df_cluni)

