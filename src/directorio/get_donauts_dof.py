# coding=utf-8
import logging

from pandas import read_csv
from pathlib import Path

from helpers.helpers import *

from typing import NoReturn


logger = logging.getLogger("donauts_dof")
logger.setLevel(logging.INFO)
logger.propagate = False

formatter = logging.Formatter("%(asctime)s [%(name)s] - %(message)s")

fh = logging.FileHandler("./logs/donaut_dof.log")
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

logger.addHandler(fh)


def get_donauts_dof(year="2020", from_dof=False):

    logger.info("Getting Donatarias Autorizadas directory from DOF")
    DATA_PATH = "./resources/data/donaut-dof/"
    Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

    files = check_sources(from_dof, DATA_PATH, "csv", logger, year=year)

    df, file = get_source("donauts-dof", files, DATA_PATH, "pdf", "csv", logger, year)

    logger.info(f"Data loaded from directory: {file}")

    logger.info("Applying filters, cleaning and formatting")
    df = df[["RFC", "sat_razon_social"]]
    df = df[
        ~((df["RFC"] == "RFC") & (df.sat_razon_social == "Denominación Social"))
    ].reset_index(drop=True)
    df = df[df["RFC"].str.len().isin([0, 12])].reset_index(drop=True)

    df["sat_razon_social"] = df.sat_razon_social.str.replace("\n", " ")

    #  check if razon social was splitted in two, row merge needed
    df[["merge_needed"]] = 1 * (df.sat_razon_social.shift(-1).str.len() == 0) + 1 * (
        df.sat_razon_social.shift(1).str.len() == 0
    )
    df[["merge_needed"]] = df.merge_needed.shift(-1) + df.merge_needed.shift(1)
    for i in df[df.merge_needed == 2].index:
        df.loc[i, "sat_razon_social"] = (
            df.loc[i - 1, "sat_razon_social"] + " " + df.loc[i + 1, "sat_razon_social"]
        )

    df = df[df["RFC"].str.len() == 12]
    df = df[df["RFC"].str.isupper()]
    df = df[~df["RFC"].str.isalpha()].reset_index(drop=True)
    df = df.drop_duplicates("RFC").reset_index(drop=True)
    df = df.drop(columns=["merge_needed"])

    df["dof_year"] = str(year)
    df.columns = df.columns.str.lower()

    logger.info("Donatarias Autorizadas directory created\n")

    return df
