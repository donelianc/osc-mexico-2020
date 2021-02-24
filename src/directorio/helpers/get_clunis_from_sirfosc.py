from json import load
from requests import request
from datetime import datetime as dt
from pandas import DataFrame

def get_clunis_from_sirfosc(path, save_response=True, filters=None):

    if filters == None:
        # use default parameters (check json file)
        with open("./params/sirfosc.json", "r") as f: params = load(f)

        BASE = 'http://www.sii.gob.mx/portal/organizaciones/excel/?'
        PARAMS = '&'.join([p.lower()+'='+params[p] for p in params.keys()])

        payload = {}
        headers = {}

        response = request("GET", BASE + PARAMS, headers=headers, data=payload)
        now = str(dt.now())[:19].replace(' ', '-').replace(':', '-')

        if save_response:
            with open(path + f'report-rfosc-{now}.txt', "w+") as f:
                f.write(response.text)
        
        df = DataFrame([row.split('","') for row in response.text.split('\n')])
        df[0] = df[0].str.replace('"', '')
        df[df.shape[1]-1] = df[df.shape[1]-1].str.replace('"\r', '')
        df = df.rename(columns=df.iloc[0]).iloc[1:]
        df = df[:-1].reset_index(drop=True)

        return(df, now)
    
    else:
        # todo: build function to retrieve GET request using user's parameters
        return(0)
    