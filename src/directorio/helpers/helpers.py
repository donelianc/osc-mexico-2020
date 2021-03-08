from glob import glob
from pandas import to_datetime, DataFrame
from helpers.get_clunis_from_sirfosc import *
from helpers.get_donauts_from_sat import *
from helpers.get_donauts_from_dof import *


def check_sources(from_source, dir_path, extension, log):

    log.info("Checking if directory already exists")
    assert dir_path[-1] == "/", "make sure path ends with /"
    assert extension[0] == ".", "make extension starts with ."

    check = dir_path + "*" + extension

    if from_source:
        log.info("Directory will be created from source")
        return []
    else:
        try:
            fs = glob(check)
            log.info(f"{len(fs)} source(s) were found")
        except Exception as e:
            log.error("no directory not found")

    return fs


def get_source(source, fs, path, extension, log):

    log.info("Looking for sources created prev")
    assert source in [
        "clunis",
        "donauts-sat",
        "donauts-dof",
    ], "Only ['clunis', 'donauts-sat'] are available"
    assert isinstance(fs, list), "files isn't a list"

    n = len(fs)

    if n == 1:
        log.info("One directory found, loading data from it")
        file = str(fs[0])

    elif n > 1:
        log.info("Multiple directories found, loading data from most recent")
        dts = dict(
            [(dt, to_datetime(dt[-23:-4], format="%Y-%m-%d-%H-%M-%S")) for dt in fs]
        )
        dates = list(dts.values())
        names = list(dts.keys())

        file = names[dates.index(max(dts.values()))]

    else:
        log.info("No sources found, downloading from original")
        try:
            if source == "clunis":
                df, now = get_clunis_from_sirfosc(path, extension)
                file = f"{path}{extension[1:]}/{now}{extension}"
            elif source == "donauts-sat":
                df, now = get_donauts_from_sat(path[-5:-1], path)
                file = f"{path}/{now}{extension}"
            elif source == "donauts-dof":
                df, now = get_donauts_from_dof(path[-5:-1], path)
                file = f"{path}/{now}{extension}".replace("pdf", extension[1:])

        except Exception as e:
            log.error("something occurred when talking to the server")
            raise (e)

        try:
            df.to_csv(file)
            log.info(f"Data loaded from response, saved at: {file}")
        except Exception as e:
            log.error("directory creation (csv) failed")

    return file
