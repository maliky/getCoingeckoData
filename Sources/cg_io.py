# -*- coding: utf-8 -*-
# ~/Python/Env_sys/KolaVizPrj/KolaViz/Lib/fnames.py voir
from typing import Union, Dict
from pathlib import Path
import os
import os.path as op

from pickle import dump, load
from pandas import DataFrame, Series, read_csv, read_json, to_datetime

from Sources.cg_logging import logger  #


def save_data(obj, fileout, logLevel=None):
    """Save the objet in the fileout"""
    if logLevel is not None:
        getattr(logger, logLevel.lower)(f"Saving {type(obj)} in {fileout}")

    with open(fileout, "bw") as fd:
        dump(obj, fd)


def load_data(filename, logLevel=None):
    """Charge filename in memory"""
    if logLevel is not None:
        getattr(logger, logLevel.lower())(f"Loading data from {filename}")

    with open(filename, mode="br") as fd:
        D = load(fd)

    return D


def save_data_with_ext(filename, df, mode, logLevel=None):
    """Save a df filename checking correct mode if pkl. logLevel in small"""
    if filename.suffix == ".pkl":
        assert "b" in mode

    fd = open(filename, mode)
    {
        ".pkl": save_data_with_pkl,
        ".cvs": save_data_with_csv,
        ".json": save_data_with_json,
    }[filename.suffix](df, fd)
    fd.close()

    if logLevel is not None:
        getattr(logger, logLevel.lower())(f"WROTE ({mode}, len(df)={len(df)}) to {filename} with SUCCESS!")
    return True


def save_data_with_pkl(df, fd):
    """Handle the case of"""
    dump(df, fd)


def save_data_with_csv(df, fd):
    """Handle the case of"""
    df.to_csv(fd)


def save_data_with_json(df, fd):
    """Handle the case of"""
    df.to_json(fd)


def load_with_ext(
    fname: Path, mode="br", logLevel=None
) -> Union[DataFrame, Series, Dict]:
    """
    Charge en mémoire un fichier en utilisant son extension
    pour savoir quelle méthode utiliser pour le lire.

    Open read and close the file
    """
    df = {
        ".pkl": load_with_ext_pkl,
        ".csv": load_with_ext_csv,
        ".json": load_with_ext_json,
    }[fname.suffix](fname, mode)

    if logLevel is not None:
        getattr(logger, logLevel.lower())(
            f"LOADED a (size {len(df)}) {type(df)} from {fname}"
        )

    return df


def load_with_ext_pkl(fname, mode) -> Union[DataFrame, Series, Dict]:
    """Load a pkl file into a df, series ou dict"""
    assert Path(fname).suffix == ".pkl"
    # import ipdb; ipdb.set_trace()

    try:
        fd = open(fname, mode="br" if "b" in mode else "r")
        _load = load(fd)
        fd.close()
    except EOFError as eofe:
        logger.exception(
            f"fname={fname}, mode={mode}, tell={fd.tell()}, size={op.getsize(fname)} "
        )
        fd.close()
        raise eofe
    return return_df_s_dict(_load)


def return_df_s_dict(obj) -> Union[DataFrame, Series, Dict]:
    """Make sure the object is of type df, serie or dict"""
    assert isinstance(obj, (DataFrame, Series, dict)), f"type(obj)={type(obj)}"
    if type(obj) is DataFrame:
        return DataFrame(obj)
    if type(obj) is Series:
        return Series(obj)
    else:
        return dict(obj)


def load_with_ext_csv(fname, mode) -> Union[DataFrame, Series, Dict]:
    """
    Load a json file into a df, series ou dict
    """
    assert Path(fname).suffix == ".csv"
    with open(fname, mode) as fd:
        df = read_csv(fd)
        idx_cols = df.columns[:2]
        df = df.set_index(idx_cols)
        assert idx_cols[0] == "ts"
        try:
            assert idx_cols[1] == "coins"
        except AssertionError as ae:
            logger.exception(f"{ae} so idx_cols={idx_cols}")
            df.columns.values[1] = "coins"

    return return_df_s_dict(df)


def load_with_ext_json(fname, mode) -> Union[DataFrame, Series, dict]:
    """
    Load a json file into a df, series ou dict
    """
    assert Path(fname).suffix == ".json"
    with open(fname, mode) as fd:
        try:
            df = read_json(fd)
        except Exception as e:
            logger.exception("Need a better implemenation")
            raise (e)

    return return_df_s_dict(df)


def read_local_files_in_df(
    folder=os.getcwd(), file_ext: str = ".pkl", with_details: bool = False
) -> DataFrame:
    """
    Renvois un df avec un max d'info sur les files du folder
    cols can be: fullname, mtime, atime, ctime, size, isdir, isfile, islink, ext, path, bname
    """
    logger.info(f"Reading {file_ext} files in {folder}")

    # making sure all string are coerce to Path
    folder = Path(folder)

    fullname = [
        Path(folder).joinpath(f) for f in os.listdir(folder) if f.endswith(file_ext)
    ]

    if not fullname:
        return DataFrame()

    df = DataFrame(data=fullname, columns=["fullname"])

    if with_details:
        INFOS = {
            "mtime": op.getmtime,
            "atime": op.getatime,
            "ctime": op.getctime,
            "size": op.getsize,
            "isdir": op.isdir,
            "isfile": op.isfile,
            "islink": op.islink,
            "suffix": lambda f: f.suffix,
            "extension": lambda f: f.suffix,  # left for compatibility
            "parent": lambda f: f.parent,
            "path": lambda f: op.split(f)[0],  # left for compatibility
            "stem": lambda f: f.stem,
            "bname": lambda f: op.split(f)[1],  # left for compatibility
        }

        for col, func in INFOS.items():
            df.loc[:, col] = df.fullname.apply(func)
            if "time" in col:
                df.loc[:, col] = to_datetime(df.fullname.apply(func), unit="s")
                df.loc[:, f"r{col}"] = df.loc[:, col].apply(lambda d: d.round("1s"))

    return df


def get_local_stem_from_folder(folder: str):
    """Read the file in the folder and return them withouth the extension"""
    return read_local_files_in_df(folder, with_details=True).stem
