from json import load
from time import sleep
from pathlib import Path
from requests import get
from typing import NoReturn
from camelot import read_pdf
from datetime import datetime as dt
from shutil import copyfileobj
from pandas import DataFrame, concat


def download_file(year, url, path):
    now = str(dt.now())[:19].replace(" ", "-").replace(":", "-")
    file_path = f"{now}.pdf"
    with get(url, stream=True) as r:
        with open(path + file_path, "wb") as f:
            copyfileobj(r.raw, f)

    return (now, path + file_path)


def fix_multiple_cols_name(df):
    ncols = df.shape[1]
    if ncols == 2:
        return df
    elif ncols > 2:
        # names can generate multiple columns due to bad parsing
        name_cols = list(range(1, ncols))
        df[1] = df[name_cols].apply(
            lambda row: " ".join(row.values.astype(str)), axis=1
        )
        df = df[[0, 1]]
        return df
    else:
        return DataFrame(columns=[0, 1])


def get_donauts_from_dof(year, path, save_response=True, filters=None):
    if filters == None:
        with open("./params/dof-rmf.json", "r") as f:
            params = load(f)

        publish_date = params[year]["dof"]["fecha-publicacion"]
        edition = params[year]["dof"]["edicion"]
        URL = f"https://www.dof.gob.mx/abrirPDF.php?\
            archivo={publish_date}-{edition}.pdf&anio={year}&repo=repositorio/"

        Path(path).mkdir(parents=True, exist_ok=True)
        now, file_path = download_file(year, URL, path)

        #  parse pdf to dataframe using camelot
        df = DataFrame(columns=[0, 1])

        # retriving parsing parameters to identify tables within first page
        first = params[year]["parse"]["start"]["pages"]
        start_tl = params[year]["parse"]["start"]["top-left"]
        start_br = params[year]["parse"]["start"]["bottom-right"]

        first_page = read_pdf(
            file_path,
            flavor="stream",
            table_areas=[",".join(start_tl + start_br)],
            pages=first,
        )

        df = concat([df, fix_multiple_cols_name(first_page[0].df)])

        # retriving parsing parameters to identify tables within middle pages
        full = params[year]["parse"]["full"]["pages"]
        full_tl = params[year]["parse"]["full"]["top-left"]
        full_br = params[year]["parse"]["full"]["bottom-right"]
        full_pages = read_pdf(
            file_path,
            flavor="stream",
            table_areas=[",".join(full_tl + full_br)],
            pages=full,
        )

        #  parsing last page where osc end (truncated)
        last = params[year]["parse"]["end"]["pages"]
        end_tl = params[year]["parse"]["end"]["top-left"]
        end_br = params[year]["parse"]["end"]["bottom-right"]
        last_page = read_pdf(
            file_path,
            flavor="stream",
            table_areas=[",".join(end_tl + end_br)],
            pages=last,
        )

        df = concat([df, fix_multiple_cols_name(last_page[0].df)])

        for n in range(len(full_pages)):
            aux = fix_multiple_cols_name(full_pages[n].df)
            df = concat([df, aux])

        df = df.rename(columns={0: "RFC", 1: "sat_razon_social"})

        return (df, now)

    else:
        # TODO: build function to retrieve GET request using user's parameters
        NoReturn