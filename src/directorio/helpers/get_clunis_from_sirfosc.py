from pathlib import Path
from json import load
from typing import NoReturn
from datetime import datetime as dt
from pandas import read_csv
from numpy import nan

from helpers.pretty_download import pretty_download


def get_clunis_from_sirfosc(dir_path, log, filters=None):

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

        now = str(dt.now())[:19].replace(" ", "-").replace(":", "-")
        file_name = f"{now}.txt"

        Path(dir_path).mkdir(parents=True, exist_ok=True)
        Path(dir_path.replace("txt", "csv")).mkdir(parents=True, exist_ok=True)
        out_path = pretty_download(
            url=BASE + PARAMS, path=dir_path + file_name, log=log
        )

        df = read_csv(
            out_path,
            delimiter='","',
            encoding="latin-1",
            engine="python",
        )

        df[df.columns[0]] = df[df.columns[0]].str.replace('"', "")
        df[df.columns[-1]] = (
            df[df.columns[-1]]
            .str.replace('"', "")
            .str.replace('"\r', "")
            .replace("NA", nan)
        )

        df.columns = [col.replace('"', "") for col in df.columns]

        return (df, out_path)

    else:
        # TODO: build function to retrieve GET request using user's parameters
        NoReturn
