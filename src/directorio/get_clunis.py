# coding=utf-8
import logging

from json import load
from pandas import read_csv
from pandas import to_datetime

from pathlib import Path
from helpers.helpers import *


logger = logging.getLogger("cluni")
logger.setLevel(logging.INFO)
logger.propagate = False

formatter = logging.Formatter("%(asctime)s [%(name)s] - %(message)s")

fh = logging.FileHandler("./logs/cluni.log")
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

logger.addHandler(fh)


def get_clunis(
    columns=None,
    from_sirfosc=False,
    status=["ACTIVA", "INACTIVA"],
    representation=["VIGENTE", "VENCIDA"],
):

    logger.info("Getting CLUNI directory")

    DATA_PATH = "./resources/data/sirfosc/"
    FORMAT_PATH = "./format/"

    Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

    files = check_sources(from_sirfosc, DATA_PATH, "csv", logger)

    df, file = get_source("clunis", files, DATA_PATH, "txt", "csv", logger)

    logger.info(f"Data loaded from directory: {file}")

    logger.info("Applying filters, cleaning and formatting")
    df_cluni = df[
        df["ESTATUS"].isin(status)  # filter osc by cluni status
        & df["ESTATUS DE LA REPRESENTACION"].isin(
            representation
        )  # filter osc by rfosc representation
    ]
    df_cluni = df_cluni[
        df_cluni.RFC.str.len().isin([12, 13])
    ]  # removing osc with bad formatted rfc
    df_cluni = df_cluni.reset_index(drop=True)  # reset index due to removing entries

    if columns is None:  # cleaning and formatting
        Path(FORMAT_PATH).mkdir(parents=True, exist_ok=True)
        with open("./format/cluni.json", "r") as f:
            c = load(f)

    col_names = c["cols"].keys()
    new_col_names = [(k, v["name"]) for k, v in c["cols"].items()]
    new_col_types = [
        (k, v["type"]) for k, v in c["cols"].items() if v["type"] in ["str", "int"]
    ]

    df_cluni.astype(new_col_types)

    for k, v in c["cols"].items():
        if v["type"] == "date":
            df_cluni[k] = to_datetime(
                df_cluni[k]
            )  # todo: there's better way to this with multiple columns?

    cols_to_lower = [
        k for k, v in c["cols"].items() if v["lower"]
    ]  #  to lower values from certain columns
    df_cluni[cols_to_lower] = df_cluni[cols_to_lower].apply(lambda c: c.str.lower())

    logger.info("Renaming columns and preparing dataset")
    df_cluni = df_cluni[col_names].rename(
        columns=dict(new_col_names)
    )  #  rename columns

    logger.info("CLUNI direectory created\n")

    return df_cluni
