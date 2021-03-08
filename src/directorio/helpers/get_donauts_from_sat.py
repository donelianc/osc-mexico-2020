from json import load
from typing import NoReturn

from requests import get
from requests import Session
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from datetime import datetime as dt
from pandas import read_excel


def get_donauts_from_sat(year, dir_path, save_response=True, filters=None):

    year

    if filters == None:
        # use default parameters (check json file)
        with open("./params/directorio-sat.json", "r") as f:
            params = load(f)

        extension = params[year]["file_extension"]

        BASE = params[year]["base_url"]
        URL = BASE + year + extension

        response = get(URL)

        # TODO: SAT serve might be so unstable, need to handle 500 errors
        # ===== from: https://stackoverflow.com/a/35504626 ===== #
        #        headers = {
        #             'Upgrade-Insecure-Requests': '1'
        #             , 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
        #             , 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
        #            }
        #
        #        s = Session()
        #        retries = Retry(
        #            total=5
        #            , backoff_factor=1
        #            , status_forcelist=[ 500, 502, 503, 504 ]
        #            )
        #
        #        s.mount('http://', HTTPAdapter(max_retries=retries))
        #        response = s.get(URL, headers=headers)
        # ===== end ===== #

        now = str(dt.now())[:19].replace(" ", "-").replace(":", "-")

        file_path = f"{year}-{now}{extension}"

        if save_response:
            with open(dir_path + file_path, "wb") as f:
                f.write(response.content)

        df = read_excel(
            dir_path + file_path,
            skiprows=range(params[year]["skip_rows"][0], params[year]["skip_rows"][1]),
            usecols=params[year]["usecols"],
        )

        df = df.rename(columns=df.iloc[0]).iloc[1:].reset_index(drop=True)

        return (df, now)

    else:
        # todo: build function to retrieve GET request using user's parameters
        NoReturn
