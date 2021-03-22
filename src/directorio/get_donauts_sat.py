# coding=utf-8
import logging

from pandas import read_csv
from pandas import to_datetime

from helpers.helpers import *


logger = logging.getLogger("donauts_sat")
logger.setLevel(logging.INFO)
logger.propagate = False

formatter = logging.Formatter("%(asctime)s [%(name)s] - %(message)s")

fh = logging.FileHandler("./logs/donaut_sat.log")
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

logger.addHandler(fh)


def get_donauts_sat(year="2020", from_sat=False):

    logger.info("Getting Donatarias Autorizadas directory from SAT")

    DATA_PATH = "./resources/data/donaut-sat/"
    Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

    files = check_sources(from_sat, DATA_PATH, "csv", logger, year=year)

    df, file = get_source("donauts-sat", files, DATA_PATH, "sheet", "csv", logger, year)

    logger.info(f"Data loaded from directory: {file}")

    logger.info("Applying filters, cleaning and formatting")
    df = df.drop_duplicates("RFC")[  #  drop duplicates for RFC and renaming
        ["RFC", "FECHA DE OFICIO"]
    ].rename(columns={"RFC": "rfc", "FECHA DE OFICIO": "fecha_oficio_sat"})

    df = df[  # removing osc with bad formatted rfc
        df.rfc.str.len().isin([12, 13])
    ].reset_index(drop=True)

    df[
        "fecha_oficio_sat"
    ] = to_datetime(  # TODO: formating and data cleaning for entire dataset
        df.fecha_oficio_sat
    )

    df["sat_year"] = str(year)
    df.columns = df.columns.str.lower()

    logger.info("Donatarias Autorizadas directory created\n")

    return df
