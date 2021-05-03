# -*- coding: utf-8 -*-
from pathlib import Path
from time import sleep
from typing import Sequence, List
from argparse import ArgumentParser
from sys import exit
import os

from pandas import concat, Timestamp, DataFrame, TimeDelta, Series
from pycoingecko.api import CoinGeckoAPI

from cg_myschedule import SafeScheduler
from cg_io import load_with_ext, save_data_with_ext, read_local_files_in_df
from cg_lib import check_mode
from cg_settings import DATEGENESIS, DFT_OLDAGE
from cg_logging import logger
from cg_times import _now, ts_extent
from cg_api import get_coins_list, w_get_coin_market_chart_range_by_id


# TODO: permettre l'update d'une plage de coins (eg. de bit.. à coin..)
# revoir le type de args.folder and folder

def download_coinid_for_date_range(
    cg: CoinGeckoAPI,
    coinid: str,
    folder: Path,
    file_ext: str = ".pkl",
    from_tsh: Timestamp = DATEGENESIS,
    to_tsh: Timestamp = _now(),
    vs_currency: str = "usd",
    mode: str = "bw",
) -> DataFrame:
    """
    download and save the data for coinid
    if overwrite is true, or if now file for coinid.ext exist,
    download all data from oldest date to _now
    else, if a file for the coinid  exist, open it and update it if older than timedelta
    I have 3 mode:
    - x only create new file, raise if exisiting untouch
    - w rewrite datafile with given tsh (human ts)
    - a+ update data of datafile
    """
    filename = folder.joinpath(f"{coinid}{file_ext}")

    assert check_mode(mode, file_ext)

    kwargs = {
        "cg": cg,
        "_id": coinid,
        "vs_currency": vs_currency,
        "from_tsh": from_tsh,
        "to_tsh": to_tsh,
    }
    df = None
    if os.path.exists(filename):
        if "a" in mode or "w" in mode:
            previous_df = None  # in case of update
            if "w" in mode:
                logger.info(f"{filename} exist already, we REWRITE it.")

            if "a" in mode:
                previous_df = load_with_ext(filename, mode)
                kwargs["from_tsh"] = ts_extent(previous_df)[1]
                if kwargs["from_tsh"] is None:
                    kwargs["from_tsh"] = DATEGENESIS
                else:
                    assert kwargs["from_tsh"] < to_tsh

                logger.info(
                    f"we UPDATE {filename} from {kwargs['from_tsh']} to {to_tsh}"
                )

            df = w_get_coin_market_chart_range_by_id(**kwargs)
            df = concat([previous_df, df])  # in case of an update
            save_data_with_ext(filename, df, mode)
    else:
        if "x" in mode:
            logger.info(f"{filename} did not exist already, we CREATE it.")
            df = w_get_coin_market_chart_range_by_id(**kwargs)
            save_data_with_ext(filename, df, mode)

    return df


def are_valide_coin_ids(coins_ids: Sequence[str], ids: Sequence[str]) -> bool:
    """check that ids are coins ids"""


def update_coins_histdata(
    cg: CoinGeckoAPI,
    fileins: Sequence[Path],
    to_date: Timestamp = _now(),
    vs_currency: str = "usd",
) -> None:
    """Met à jour les fileins avec des données to_date"""
    for fi in fileins:
        _ = download_coinid_for_date_range(
            cg,
            fi.stem,
            fi.parent,
            file_ext=fi.suffix,
            to_tsh=to_date,
            mode="ba" if fi.suffix == ".pkl" else "a",
            vs_currency=vs_currency,
        )


def update_aged_histdata(
    cg: CoinGeckoAPI,
    folder: Path,
    file_ext: str,
    to_date: Timestamp = _now(),
    age: TimeDelta = DFT_OLDAGE,
    vs_currency: str = "usd",
):
    """Update data of files of from folder that are older than DFT_OLDAGE"""
    dataFiles = read_local_files_in_df(folder, file_ext, with_details=True)

    def _old():
        return dataFiles.ctime < (_now() - DFT_OLDAGE)

    agedDataFiles = dataFiles.where(_old).dropna().fullname

    return update_coins_histdata(cg, agedDataFiles, to_date, vs_currency)


def update_histdata(
    cg: CoinGeckoAPI,
    folder: Path,
    file_ext: str,
    how: str = "all",
    to_date: Timestamp = _now(),
    vs_currency: str = "usd",
):
    """
    Get all existing files of ext type in folder and update them to tsh
    how should be a keyword, specifing what file to update.  f10-old, l10-size,
    for First 10 of the oldest, or all-old. or all-mtime
    """
    # def parse_how():

    dataFiles = read_local_files_in_df(folder, file_ext)
    return update_coins_histdata(cg, dataFiles, to_date, vs_currency)


def create_coins_histdata(
    cg: CoinGeckoAPI,
    folder: Path,
    file_ext: str = ".pkl",
    to_date: Timestamp = _now(),
    vs_currency: str = "usd",
) -> None:
    """
    Get existing files and coinid, compare to see what are the missing files
    download the data to create them.
    """
    dataFiles = read_local_files_in_df(folder, file_ext)
    local_coinids = [f.stem for f in dataFiles]
    coins_ids = get_coins_list(cg, update_local=False)

    assert len(coins_ids) >= len(
        local_coinids
    ), f"{set(local_coinids) - set(coins_ids)}"
    new_coinids = set(coins_ids) - set(local_coinids)

    # Create new coinids data file on disk
    for coinid in new_coinids:
        _ = download_coinid_for_date_range(
            cg,
            coinid,
            folder,
            file_ext=file_ext,
            to_tsh=to_date,
            vs_currency=vs_currency,
            mode="bx" if file_ext == ".pkl" else "x",
        )


def renew_all_histdata(
    cg: CoinGeckoAPI,
    folder: Path,
    file_ext: str = ".pkl",
    to_date: Timestamp = _now(),
    vs_currency: str = "usd",
) -> None:
    """Rewrite all database with data up 'to_date'"""
    new_coin_ids = get_coins_list(cg, update_local=True)
    for coinid in new_coin_ids:
        _ = download_coinid_for_date_range(
            cg,
            coinid,
            folder,
            file_ext=file_ext,
            to_tsh=to_date,
            vs_currency=vs_currency,
            mode="bw" if file_ext == ".pkl" else "w",
        )


def create_all_histdata(
    cg: CoinGeckoAPI,
    folder: Path,
    file_ext: str = ".pkl",
    to_date: Timestamp = _now(),
    vs_currency: str = "usd",
) -> None:
    """Rewrite all database with data up 'to_date'"""
    new_coin_ids = get_coins_list(cg, update_local=True)
    for coinid in new_coin_ids:
        _ = download_coinid_for_date_range(
            cg,
            coinid,
            folder,
            file_ext=file_ext,
            to_tsh=to_date,
            vs_currency=vs_currency,
            mode="bx" if file_ext == ".pkl" else "x",
        )


def parse_args():
    """Parse command line arguments"""
    # description, defaults and help
    description = (
        """Application to download and update all coin listed by of coingecko"""
    )
    action_dft = "UPDATE-ALL"
    action_help = "UPDATE-ALL, CREATE-ALL, UPDATE-COINS, CREATE-COINS, GET-COINS, GET-ALL, LIST-COINS"

    coins_ids_dft = "bitcoin,cardano"
    coins_ids_help = (
        " Specify which coin to get.  Can be a coinid (see action LIST-COINS)"
        f", a list of coinid 'id1,id2,id3' or a range 'idx-idy' {coins_ids_dft}"
    )

    coins_order_dft = "alpha"
    coins_order_hlp = (
        f"Define the order in which to update coins (def. {coins_order_dft}).  "
        "should be one of alpha or mtime or another os.path propertie supported."
    )

    coins_strip_dft = "f10"
    coins_strip_hlp = (
        f"Define the number of coin to update.  Should be a letter f or"
        f" l for first or last and a number. (def. {coins_strip_dft})."
    )
    sort_dft = "alphabetical"
    sort_help = "a sort order to handle coins before action, can be market-cap, price, mtime, size"

    folder_dft = Path("./data/historical-capitalisation")
    folder_help = f"Name of the data folder root (def. {folder_dft.as_posix()})"

    filefmt_dft = ".pkl"
    filefmt_help = f"file format to save or read the data from. ({filefmt_dft})"

    vsCurrency_dft = "usd"
    vsCurrency_help = f"The currency in wich to show prices. ({vsCurrency_dft})"

    logLevel_dft = "INFO"
    logLevel_help = f"Set the log level (def. {logLevel_dft})"

    overwrite_help = "Overwrite previously downloaded data"

    parser = ArgumentParser(description)
    parser.add_argument("--Order", "--O", default=coins_order_dft, help=coins_order_hlp)
    parser.add_argument("--Strip", "-S", default=coins_strip_dft, help=coins_strip_hlp)
    parser.add_argument("--logLevel", "-L", help=logLevel_help, default=logLevel_dft)
    parser.add_argument("--coins", "-c", help=coins_ids_help, default=coins_ids_dft)
    parser.add_argument("--sort", "-s", help=sort_help, default=sort_dft)
    parser.add_argument(
        "--vsCurrency", "-v", help=vsCurrency_help, default=vsCurrency_dft
    )
    parser.add_argument("--filefmt", "-f", help=filefmt_help, default=filefmt_dft)
    parser.add_argument("--folder", "-d", help=folder_help, default=folder_dft)
    parser.add_argument("--overwrite", "-o", action="store_true", help=overwrite_help)
    parser.add_argument("--action", "-a", help=action_help, default=action_dft)

    return parser.parse_args()


def main_prg():
    """
    Run the main programme.
    It connects to Coingecko API, and dowload or update capitalisation data
    Save the data on disk.
    """
    args = parse_args()

    logger.setLevel(args.logLevel)
    logger.info(f"Starting main programme with {args} ")

    os.makedirs(args.folder, exist_ok=True)

    cg = CoinGeckoAPI()
    logger.info(f"{args.action}")
    # action_help = "UPDATE-ALL, CREATE-ALL, RENEW-ALL, UPDATE-COINS, CREATE-COINS, LIST-COINS"

    if args.action.upper() == "UPDATE-ALL":
        update_time = "18:50"
        kwargs = {
            "folder": args.folder,
            "file_ext": args.filefmt,
            "vs_currency": args.vsCurrency,
        }
        logger.info(f"Updating and then runing a daily update at {update_time}")

        scheduler = SafeScheduler()
        update_job = (
            scheduler.every(1)
            .day.at(update_time)
            .do(update_histdata, cg, **kwargs)
            .tag("update")
        )
        update_job.run()

        while True:
            scheduler.run_pending()
            sleep(1)

    elif args.action.upper() == "CREATE-ALL":
        create_all_histdata(
            cg, folder=args.folder, file_ext=args.filefmt, vs_currency=args.vsCurrency,
        )
    elif args.action.upper() == "RENEW-ALL":
        renew_all_histdata(
            cg, folder=args.folder, file_ext=args.filefmt, vs_currency=args.vsCurrency,
        )

    elif args.action.upper() == "UPDATE-COINS":
        fileins = parse_coins_id_to_filename(args.coins, args.folder, args.filefmt)
        update_coins_histdata(cg, fileins, to_date=_now(), vs_currency="usd")
    elif args.action.upper() == "RENEW-COINS":
        logger.info("Creating new data base")
        kwargs = {
            "folder": args.folder,
            "file_format": args.filefmt,
            "mode": "bw" if args.overwrite else "bx",
        }

        _ = download_coinid_for_date_range(cg, **kwargs)
        return None


def parse_coins_id_to_filename(
    cg: CoinGeckoAPI, args_coins, folder: Path, file_ext: str = ".pkl"
) -> List[Path]:
    """Parse a list of coins and return a set of filename to processe
    """
    # should I make a specific currency folder?
    coins_ids = get_coins_list(cg, update_local=False)

    ret_ids = []
    for arg_coin in args_coins.split(","):
        if "-" in arg_coin:
            ret_ids += parse_plage_of_coin(coins_ids, arg_coin)
        else:
            ret_ids += [arg_coin]

    # check the validity of the returned_ids
    left_ids = set(ret_ids) - set(coins_ids)
    assert (
        len(left_ids) == 0
    ), f"{ret_ids} and {coins_ids}, diff {set(ret_ids) - set(coins_ids)}"

    return [folder.joinpath(f"{a}{file_ext}") for a in ret_ids]


def parse_plage_of_coin(coins_ids: Series, arg_coin: str):
    """ given a string in the form a-d get all coin in between in alphabetical order"""
    extremum = arg_coin.split("-")
    assert len(extremum) == 2, f"{arg_coin}"
    a, b = sorted(extremum)

    # sx = IndexSlice
    return coins_ids.loc[a:b]


if __name__ == "__main__":
    main_prg()
    exit()
