from json import load
from pathlib import Path
from typing import NoReturn

from requests import exceptions

from helpers.pretty_download import pretty_download

from datetime import datetime as dt
from pandas import read_excel


def get_donauts_from_sat(year, path, filters=None, log=None):

    if filters == None:
        # use default parameters (check json file)
        with open("./params/directorio-sat.json", "r") as f:
            params = load(f)

        extension = params[year]["file_extension"]

        BASE = params[year]["base_url"]
        URL = BASE + year + extension
        log.info(f"Download directorio from SAT using: {URL}")

        now = str(dt.now())[:19].replace(" ", "-").replace(":", "-")
        file_name = f"{year}-{now}{extension}"

        path += f"{year}/"

        Path(path).mkdir(parents=True, exist_ok=True)
        Path(path.replace("sheet", "csv")).mkdir(parents=True, exist_ok=True)
        out_path = pretty_download(url=URL, path=path + file_name, log=log)

        try:
            df = read_excel(
                path + file_name,
                skiprows=range(
                    params[year]["skip_rows"][0], params[year]["skip_rows"][1]
                ),
                usecols=params[year]["usecols"],
            )
        except Exception as e:
            log.error("Something occurred when talking to the server.")
            raise e

        df = df.rename(columns=df.iloc[0]).iloc[1:].reset_index(drop=True)

        return (df, out_path)

    else:
        # todo: build function to retrieve GET request using user's parameters
        NoReturn
