# -*- coding: utf-8 -*-
from pathlib import Path
from time import sleep
from typing import List, Optional
from argparse import ArgumentParser
from sys import exit
import os
from os.path import exists, getsize

from pandas import concat, Timestamp, DataFrame, Timedelta, Series
from pycoingecko.api import CoinGeckoAPI

from Sources.cg_logging import logger  #
from Sources.cg_times import now_as_ts, ts_extent  #
from Sources.cg_settings import DATEGENESIS  #
from Sources.cg_scheduling import SafeScheduler  # log
from Sources.cg_io import (
    load_with_ext,
    save_data_with_ext,
    read_local_files_in_df,
)  # log

from Sources.cg_lib import (
    is_old,
    check_mode,
    get_coins_list,
    w_get_coin_market_chart_range_by_id,
)  # log, set, time, io, deco, fmt

# TODO: permettre l'update d'une plage de coins (eg. de bit.. à coin..)
# revoir le type de args.folder and folder


def download_coinid_for_date_range(
    cg: CoinGeckoAPI,
    coinid: str,
    folder: Path,
    file_ext: str = ".pkl",
    from_tsh: Timestamp = DATEGENESIS,
    to_tsh: Timestamp = now_as_ts(),
    vs_currency: str = "usd",
    mode: str = "bw",
) -> DataFrame:
    """
    download and save the data for coinid
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
        "id_": coinid,
        "vs_currency": vs_currency,
        "from_ts": from_tsh,
        "to_ts": to_tsh,
    }
    df = None
    if exists(filename):
        if "w" in mode or "+" in mode:
            previous_df = DataFrame(None)  # in case of update
            # logger.info(f"EXISTS *{filename.stem}*, size {getsize(filename)}")

            if "+" in mode and (getsize(filename) != 0):

                # we load the file from the disk
                previous_df = DataFrame(load_with_ext(filename, mode, "info"))

                old_from_ts, kwargs["from_ts"] = ts_extent(previous_df)
                if kwargs["from_ts"] is None:
                    kwargs["from_ts"] = DATEGENESIS
                else:
                    assert kwargs["from_ts"] < to_tsh
                    logger.info(
                        f"OLD ts *{filename.stem}* {(old_from_ts, kwargs['from_ts'])}"
                    )
                # we change the a in w...
                mode = mode.replace("a", "w")

            # we get the data from API
            _df = DataFrame(w_get_coin_market_chart_range_by_id(**kwargs))

            # add it to previous if we do an update
            df = concat([previous_df, _df])
            logger.info(
                f"UPDATING *{filename.stem}* with {len(_df)}-{ts_extent(_df)} "
                f"to {len(df)}-{ts_extent(df)}"
            )
            # and write it on disk
            save_data_with_ext(filename, df, mode, "info")
    else:
        if "x" in mode:
            df = DataFrame(w_get_coin_market_chart_range_by_id(**kwargs))
            logger.info(f"*{filename.stem}*\t CREATING with {len(df)}-{ts_extent(df)}")
            save_data_with_ext(filename, df, mode, "info")
    logger.info(f"DOWNLOAD FINISHED *{filename.stem}*")
    return DataFrame(df)


def update_coins_histdata(
    cg: CoinGeckoAPI,
    folder: Path,
    file_ext: str = ".pkl",
    to_date: Timestamp = now_as_ts(),
    vs_currency: str = "usd",
    age: Optional[Timedelta] = None,
    fileins: Optional[List[Path]] = None,
) -> None:
    """Met à jour les fileins avec des données to_date"""
    if fileins is None:

        fileins = read_local_files_in_df(folder, file_ext, with_details=True).fullname
        assert (
            len(fileins) > 0
        ), f"folder={folder}, file_ext={file_ext}. No files to update please use CREATE"

    if age is not None:
        logger.info(f"UPDATING files in {folder} CHANGED more than {age} ago.")
        mask = map(is_old, fileins)
        fileins = Series(fileins).loc[mask]

    logger.info(f"UPDATING {len(fileins)} files to {to_date}")

    for (i, fi) in enumerate(fileins):
        logger.info(f"{i+1}/{len(fileins)}: Updating {fi}")
        _ = download_coinid_for_date_range(
            cg,
            fi.stem,
            fi.parent,
            file_ext=fi.suffix,
            to_tsh=to_date,
            mode="ba+" if fi.suffix == ".pkl" else "a+",
            vs_currency=vs_currency,
        )
    logger.info(f"UPDATED {len(fileins)} files.")


def create_coins_histdata(
    cg: CoinGeckoAPI,
    folder: Path,
    file_ext: str = ".pkl",
    to_date: Timestamp = now_as_ts(),
    vs_currency: str = "usd",
    fileins: Optional[List[Path]] = None,
) -> None:
    """
    Get existing files and coinid, compare to see what are the missing files
    download the data to create them.
    """
    if fileins is None:
        new_coinids = set(get_coins_list(cg, update_local=False)) - set(
            read_local_files_in_df(folder, file_ext, with_details=True).stem
        )
    else:
        new_coinids = [f.stem for f in fileins]

    # Create new coinids data file on disk
    for (i, coinid) in enumerate(new_coinids):
        logger.info(f"{i+1}/{len(new_coinids)}:  CREATING *{coinid}*")
        _ = download_coinid_for_date_range(
            cg,
            coinid,
            folder,
            file_ext=file_ext,
            to_tsh=to_date,
            vs_currency=vs_currency,
            mode="bx" if file_ext == ".pkl" else "x",
        )


def renew_coins_histdata(
    cg: CoinGeckoAPI,
    folder: Path,
    file_ext: str = ".pkl",
    to_date: Timestamp = now_as_ts(),
    vs_currency: str = "usd",
    fileins: Optional[List[Path]] = None,
) -> None:
    """Rewrite all database with data up 'to_date'"""
    if fileins is None:
        new_coin_ids = get_coins_list(cg, update_local=True)
    else:
        new_coin_ids = [f.stem for f in fileins]

    for (i, coinid) in enumerate(new_coin_ids):
        logger.info(f"{i+1}/{len(new_coin_ids)}:  RENEWING {coinid}")
        _ = download_coinid_for_date_range(
            cg,
            coinid,
            folder,
            file_ext=file_ext,
            to_tsh=to_date,
            vs_currency=vs_currency,
            mode="bw" if file_ext == ".pkl" else "w",
        )


def parse_coins_id_to_filename(
    cg: CoinGeckoAPI, args_coins, folder: Path, file_ext: str = ".pkl", coins_ids_=None
) -> Optional[List[Path]]:
    """
    Parse a list of coins and return a set of filename to processe

    - args_coins : a small grammar to define what file shoulde be generated.
    It should be comma separated expression where each expression is either
    a valid coinid or a range of coins define by to coins id separated by a comma
    ex, bitcoin,cardano,ether-filecoin
    - folder : the base folder for the files to generate
    - file_ext : the extension of the files to generate
    """
    if args_coins is None:
        return None
    # should I make a specific currency folder?

    coins_ids = (
        get_coins_list(cg, update_local=False) if coins_ids_ is None else coins_ids_
    )

    #
    _ids = []
    for arg_coin in args_coins.split(","):
        _ids += (
            parse_plage_of_coin(coins_ids, arg_coin) if "-" in arg_coin else [arg_coin]
        )

    # check the validity of the returned_ids
    _ids = set(_ids)
    unknown_ids = _ids - set(coins_ids)
    assert len(unknown_ids) == 0, f"{_ids} and {coins_ids}, unknown_ids={unknown_ids}"

    return sorted([folder.joinpath(f"{_id}{file_ext}") for _id in _ids])


def parse_plage_of_coin(coins_ids: Series, arg_coin: str):
    """ given a string in the form a-d get all coin in between in alphabetical order"""
    plage = arg_coin.split("-")
    assert len(plage) == 2, f"{arg_coin}"

    # parsing to int and sorting
    coin_a, coin_b = sorted(plage)
    coin_a_idx = (
        coins_ids.loc[coins_ids.str.startswith(coin_a)].sort_values().index.values[0]
    )
    coin_b_idx = (
        coins_ids.loc[coins_ids.str.startswith(coin_b)].sort_values().index.values[-1]
    )

    return list(coins_ids.loc[coin_a_idx:coin_b_idx])


def parse_args():
    """Parse command line arguments"""
    # description, defaults and help
    description = (
        """Application to download and update all coin listed by of coingecko"""
    )
    action_dft = "UPDATE"
    action_help = (
        "UPDATE: check the coins on the disk update them with latest data. do so regularly\n"
        "CREATE: make sure all data on disk has the coins from the API.\n"
        "RENEW: a mix of CREATE and UPDATE.\n"
        "LIST-COINS"
    )

    coins_ids_dft = "bitcoin,cardano"
    coins_ids_help = (
        " Specify which coin to get.  Can be a coinid (see action LIST-COINS)"
        f", a list of coinid 'id1,id2,id3' or a range 'idx-idy' {coins_ids_dft}"
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

    age_dft = "23"
    age_hlp = f"The age in hours of the file to update.  (def. {age_dft})."

    parser = ArgumentParser(description)
    parser.add_argument("--age", "-a", default=age_dft, help=age_hlp)
    parser.add_argument("--logLevel", "-L", help=logLevel_help, default=logLevel_dft)
    parser.add_argument("--coins", "-c", help=coins_ids_help, default=coins_ids_dft)
    parser.add_argument("--sort", "-s", help=sort_help, default=sort_dft)
    parser.add_argument(
        "--vsCurrency", "-v", help=vsCurrency_help, default=vsCurrency_dft
    )
    parser.add_argument("--filefmt", "-f", help=filefmt_help, default=filefmt_dft)
    parser.add_argument("--folder", "-d", help=folder_help, default=folder_dft)
    parser.add_argument("--action", "-A", help=action_help, default=action_dft)

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

    fileins = parse_coins_id_to_filename(cg, args.coins, args.folder, args.filefmt)

    kwargs = {
        "folder": args.folder,
        "file_ext": args.filefmt,
        "fileins": fileins,
        "vs_currency": args.vsCurrency,
    }

    if args.action.upper() == "UPDATE":
        update_time = "18:50"
        kwargs.update({"age": Timedelta(f"{args.age}h"), "to_date": now_as_ts()})
        logger.info(f"Updating and then runing a daily update at {update_time}")

        scheduler = SafeScheduler()
        update_job = (
            scheduler.every(1)
            .day.at(update_time)
            .do(update_coins_histdata, cg, **kwargs)
            .tag("update")
        )
        update_job.run()

        while True:
            scheduler.run_pending()
            sleep(1)

    elif args.action.upper() == "CREATE":
        create_coins_histdata(
            cg, folder=args.folder, file_ext=args.filefmt, vs_currency=args.vsCurrency,
        )
    elif args.action.upper() == "RENEW":
        renew_coins_histdata(
            cg, folder=args.folder, file_ext=args.filefmt, vs_currency=args.vsCurrency,
        )

    elif args.action.upper() == "UPDATE-COINS":
        kwargs.update({"age": Timedelta(f"{args.age}h"), "to_date": now_as_ts()})
        update_coins_histdata(cg, **kwargs)

    logger.info("***The End***")
    return None


if __name__ == "__main__":
    main_prg()
    exit()
