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


def get_donauts_dof(year=2020, from_dof=False):

    logger.info("Getting Donatarias Autorizadas directory from DOF")
    DATA_PATH = "./resources/data/donaut-dof/"
    Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

    files = check_sources(from_dof, DATA_PATH + f"csv/{year}/", ".csv", logger)

    file = get_source(
        "donauts-dof",
        files,
        DATA_PATH + f"pdf/{year}/",
        ".csv",
        logger,
    )

    # drop index from csv file
    df_donaut_dof = read_csv(file, low_memory=False).iloc[:, 1:]
    logger.info(f"Data loaded from directory: {file}")

    logger.info("Applying filters, cleaning and formatting")
    df_donaut_dof = df_donaut_dof[
        ~(
            (df_donaut_dof["RFC"] == "RFC")
            & (df_donaut_dof.sat_razon_social == "Denominación Social")
        )
    ].reset_index(drop=True)
    df_donaut_dof = df_donaut_dof[
        df_donaut_dof["RFC"].str.len().isin([0, 12])
    ].reset_index(drop=True)

    df_donaut_dof["sat_razon_social"] = df_donaut_dof.sat_razon_social.str.replace(
        "\n", " "
    )

    #  check if razon social was splitted in two, row merge needed
    df_donaut_dof[["merge_needed"]] = 1 * (
        df_donaut_dof.sat_razon_social.shift(-1).str.len() == 0
    ) + 1 * (df_donaut_dof.sat_razon_social.shift(1).str.len() == 0)
    df_donaut_dof[["merge_needed"]] = df_donaut_dof.merge_needed.shift(
        -1
    ) + df_donaut_dof.merge_needed.shift(1)
    for i in df_donaut_dof[df_donaut_dof.merge_needed == 2].index:
        df_donaut_dof.loc[i, "sat_razon_social"] = (
            df_donaut_dof.loc[i - 1, "sat_razon_social"]
            + " "
            + df_donaut_dof.loc[i + 1, "sat_razon_social"]
        )

    df_donaut_dof = df_donaut_dof[df_donaut_dof["RFC"].str.len() == 12]
    df_donaut_dof = df_donaut_dof[df_donaut_dof["RFC"].str.isupper()]
    df_donaut_dof = df_donaut_dof[~df_donaut_dof["RFC"].str.isalpha()].reset_index(
        drop=True
    )
    df_donaut_dof = df_donaut_dof.drop_duplicates("RFC").reset_index(drop=True)
    df_donaut_dof = df_donaut_dof.drop(columns=["merge_needed"])

    df_donaut_dof.loc[
        :,
    ] = str(year)

    logger.info("Donatarias Autorizadas directory created\n")

    return df_donaut_dof
