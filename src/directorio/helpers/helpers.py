from glob import glob
from pathlib import Path
from pandas import to_datetime
from requests.exceptions import ReadTimeout
from helpers.get_clunis_from_sirfosc import *
from helpers.get_donauts_from_sat import *
from helpers.get_donauts_from_dof import *


def check_sources(from_source, dir_path, to_extension, log, year=""):

    log.info("Checking if directorio already exists")
    assert dir_path[-1] == "/", "make sure path ends with /"

    if year != "":
        year = f"{year}/"

    dir_path += f"{to_extension}/{year}"
    Path(dir_path).mkdir(parents=True, exist_ok=True)
    full_path = f"{dir_path}*.{to_extension}"

    if from_source:
        log.info("Directory will be created from source")
        return []
    else:
        try:
            fs = glob(full_path)
            log.info(f"{len(fs)} source(s) were found")
        except Exception as e:
            log.error("no directory not found")

    return fs


def get_source(source, fs, dir_path, from_extension, to_extension, log, year=""):

    log.info("Looking for sources created prev")
    assert source in [
        "clunis",
        "donauts-sat",
        "donauts-dof",
    ], "Only ['clunis', 'donauts-sat', 'donauts-dof'] are available"
    assert isinstance(fs, list), "files isn't a list"

    dir_path += f"{to_extension}/"

    if len(fs) == 1:
        log.info("One directory found, loading data from it")
        csv_path = str(fs[0])
        df = read_csv(csv_path, low_memory=False)

    elif len(fs) > 1:
        log.info("Multiple directories found, loading data from most recent")
        dts = dict(
            [(dt, to_datetime(dt[-23:-4], format="%Y-%m-%d-%H-%M-%S")) for dt in fs]
        )
        dates = list(dts.values())
        names = list(dts.keys())

        csv_path = names[dates.index(max(dts.values()))]
        df = read_csv(csv_path, low_memory=False)

    else:
        log.info("No sources found, downloading from original")
        try:
            dir_path = dir_path.replace(to_extension, from_extension)
            if source == "clunis":
                df, file_path = get_clunis_from_sirfosc(dir_path, log=log)
            elif source == "donauts-sat":
                df, file_path = get_donauts_from_sat(year=year, path=dir_path, log=log)
            elif source == "donauts-dof":
                df, file_path = get_donauts_from_dof(year=year, path=dir_path, log=log)

        except Exception as e:
            raise e

        try:
            csv_path = file_path.replace(from_extension, to_extension)
            df.to_csv(csv_path)
            log.info(f"Data loaded from response, original saved at: {file_path}")
        except Exception as e:
            log.error("directory creation (csv) failed")
            raise e

    return df, csv_path
