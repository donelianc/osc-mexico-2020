from pathlib import Path
from json import load
from typing import NoReturn
from requests import request
from datetime import datetime as dt
from pandas import DataFrame
from os.path import isdir
from os import makedirs


def get_clunis_from_sirfosc(path, save_response=True, filters=None):

    if filters == None:
        try:
            # use default parameters (check json file)
            with open("./params/sirfosc.json", "r") as f:
                params = load(f)
        except OSError as e:
            print("Parameters in json file weren't found. Create it first.")
            raise e

        BASE = "http://www.sii.gob.mx/portal/organizaciones/excel/?"
        PARAMS = "&".join([p.lower() + "=" + params[p] for p in params.keys()])

        response = request("GET", BASE + PARAMS)
        now = str(dt.now())[:19].replace(" ", "-").replace(":", "-")

        if save_response:
            Path(path + "/txt/").mkdir(parents=True, exist_ok=True)
            if not isdir(path + "/txt/"):
                makedirs(path)
            with open(path + "/txt/" + f"{now}.txt", "w+") as f:
                f.write(response.text)

        df = DataFrame([row.split('","') for row in response.text.split("\n")])
        df[0] = df[0].str.replace('"', "")
        df[df.shape[1] - 1] = df[df.shape[1] - 1].str.replace('"\r', "")
        df = df.rename(columns=df.iloc[0]).iloc[1:]
        df = df[:-1].reset_index(drop=True)

        return (df, now)

    else:
        # TODO: build function to retrieve GET request using user's parameters
        NoReturn