# -*- coding: utf-8 -*-
"""Program to get info about coins"""
from argparse import ArgumentParser
from pickle import dump, load
from time import sleep
from typing import Dict
import os.path
import os
from pathlib import Path

from pandas import DataFrame, Series
from pycoingecko.api import CoinGeckoAPI

from getCoingeckoData.cg_decorators import w_retry, as_pd_object
from getCoingeckoData.cg_io import get_local_stem_from_folder, read_local_files_in_df
from getCoingeckoData.cg_settings import APISLEEP
from getCoingeckoData.cg_logging import logger
from getCoingeckoData.cg_lib import get_coins_list


@w_retry()
@as_pd_object("Series")
def w_get_coin_by_id(
    cg: CoinGeckoAPI, _id: str, save: bool = False, **kwargs
) -> Series:
    """Juste a easy wrapper around the standard API function"""
    coin_by_id = cg.get_coin_by_id(_id, **kwargs)
    sleep(APISLEEP())
    if save:
        with open(f"./Coingecko_data/Coins_infos/{_id}", "bw") as fd:
            dump(coin_by_id, fd)

    return Series(coin_by_id)


@as_pd_object("DataFrame")
def download_coins_infos(
    cg: CoinGeckoAPI,
    info_folder: str = "./Coingecko_data/Coins_infos",
    to_save: bool = True,
    overwrite: bool = False,
) -> Dict:
    """Télécharge les infos des tokens dans un dossier"""

    # récupère les noms des coins
    coins_list = get_coins_list(cg)
    coins_infos = {}

    # preparation des arguments pour la requête
    # On en veut le moins possible
    kwargs = {
        k: "false" for k in ["tickers", "localization", "market_data", "sparkline"]
    }

    assert os.path.exists(
        info_folder
    ), f"{info_folder} n'existe pas dans {os.getcwdb()}"
    if not overwrite:
        # we limite the coins that we want to download
        # and download only those not on the disk already
        local_coins_list = get_local_stem_from_folder(info_folder)
        logger.info(f"Dropping {len(local_coins_list)} already downloaded coins.")
        coins_list = set(coins_list) - set(local_coins_list)

    for (i, _id) in enumerate(coins_list):

        # making the request
        coin_info = w_get_coin_by_id(cg, _id, as_series=True, **kwargs)
        coin_info.index.name = "coin_id"
        if not to_save:
            logger.info(f"{i}/{len(coins_list)} : GETTING '{_id}'")
            coins_infos[_id] = coin_info
        elif to_save:
            coin_info_fn = Path(f"{info_folder}/{_id}.pkl")

            logger.info(f"{i}/{len(coins_list)} : Saving '{coin_info_fn}'")

            # this will overwrite if called with existing coin_info_fn
            with open(coin_info_fn, "bw") as fd:
                dump(coin_info, fd)

    return coins_infos


def load_local_coins_infos(folder: str, save: bool = True) -> DataFrame:
    """Read file in folder in one DataFrame and optionaly save it to coins_infos_list.csv"""
    _df = {}
    coins_infos_list = read_local_files_in_df(folder)
    for (i, fn) in enumerate(coins_infos_list.fullname):
        print(f"loading {fn.stem} : {i+1}/{len(coins_infos_list.fullname)}", end="\r")
        with open(fn, "br") as fd:
            _df[fn.stem] = load(fd)

    df = DataFrame(_df).T
    if save:
        fn = Path(folder).parent.joinpath("coins_infos_list.csv")
        with open(fn, "bw") as fd:
            dump(df, fd)
            logger.info(f"Dumping coins infos summary in {fn}")

    df.index.name = "coin_id"
    return df


def parse_args():
    """Parse commande line argument."""
    description = (
        """Application to download information about coins listed by of coingecko"""
    )

    folder_dft = Path("./Coingecko_data")
    folder_help = f"Name of the data folder root (def. {folder_dft.as_posix()})"

    logLevel_dft = "INFO"
    logLevel_help = f"Set the log level (def. {logLevel_dft})"

    parser = ArgumentParser(description)

    parser.add_argument("--logLevel", "-L", help=logLevel_help, default=logLevel_dft)
    parser.add_argument("--folder", "-f", help=folder_help, default=folder_dft)

    return parser.parse_args()


def main_prg():
    """Télécharge les données en détail pour les coins de Coingecko"""
    args = parse_args()

    logger.setLevel(args.logLevel)
    logger.debug(f"Starting main programme with {args} ")

    os.makedirs(args.folder, exist_ok=True)
    cg = CoinGeckoAPI()

    info_folder = f"./{args.folder}/Coins_infos"

    if os.path.exists(info_folder):
        logger.info(f"Redownloading infos in {info_folder}")
    else:
        os.makedirs(info_folder, exist_ok=True)

    download_coins_infos(cg, info_folder, to_save=True, overwrite=False)
    _ = load_local_coins_infos(info_folder)


if __name__ == "__main__":
    main_prg()
    exit()
