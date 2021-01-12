# -*- coding: utf-8 -*-
"""main file getcg_data.py. pour récupérer les données avec coingecko"""
from time import sleep

import argparse
import logging
import os
import platform  # handle os check
import sys
import numpy as np

from pandas import DataFrame, date_range, to_datetime, Timestamp, concat, set_option
from pathlib import Path

import pycoingecko as pcg

logger = logging.getLogger()
logger.setLevel("INFO")

# setting the default time zone for the system
if platform.system() == "Linux":
    # Time.tzset ne fonctionne qu'avec UNIX
    OS_TZ = os.environ.get("TZ", "UTC")
else:
    # in the case os.environ does not exist
    OZ_TZ = "UTC"

STRF = "%Y-%m-%d__%H_%M"  # default time format for saving the data

# les dates
def get_ts_data(start_tsh_, end_tsh_, freq_="1d"):
    """
    Créer des bins avec les dates de départ et la fréquence freq
    Renvoi un dictionnaire avec 
    - bins, couple de date
    - h_bins, couple des dates au format humain
    - range, une liste de date de start_tsh à end_tsh
    espacé par freq_
    - h_range: comme au dessus mais avec les dates humaines
    """
    # créer deux ensembles de date de start à end
    tsh_range = date_range(start_tsh_, end_tsh_, freq=freq_)
    ts_range = list(map(lambda ts: ts.timestamp() * 10 ** 6, tsh_range))

    return {
        "bins": list(zip(ts_range[:-1], ts_range[1:])),
        "h_bins": list(zip(tsh_range[:-1], tsh_range[1:])),
        "h_range": tsh_range,
        "range": ts_range,
    }


def market_chart_range_to_df(_dict):
    """Get a result from the coingecko API et le renvois en DataFrame."""
    _R = None

    # on parcours les élèments du dictionnair
    for titre, data in _dict.items():
        _R = (
            cg_api_to_df(data, titre)
            if _R is None
            else concat([_R, cg_api_to_df(data, titre).drop("ts", axis=1)], axis=1)
        )
    return _R


def cg_api_to_df(data_, keys_="value"):
    "Transforme une liste avec une colonne de timestamp en dataframe"
    return DataFrame(
        index=to_datetime(np.array(data_).T[0] * 10 ** 6).round("s"),
        data=data_,
        columns=["ts", keys_],
    )


def coerce_ts(ts_):
    """Convertis un ts string en Timesstamp"""
    if isinstance(ts_, str):
        return Timestamp(ts_)
    if isinstance(ts_, Timestamp):
        return ts_

    raise Exception(f"Check type of ts_ {ts_}")


def getcg_market_trades(
    from_, to_, fout_=None, pause_=1.2, vs_currency_="usd", id_="cardano"
):
    """
    Execute les appels API sur coingecko et écris les resultats transformés dans le fichier fout
    """
    sess = pcg.CoinGeckoAPI()
    set_option("display.precision", 8)
    fout_ = f"cg_data-{from_.strftime(STRF)}-{to_.strftime(STRF)}.csv" if fout_ is None else fout_

    df = None

    # créer un dictionnaire avec divers objets temporels utiles
    date_couple = get_ts_data(coerce_ts(from_), coerce_ts(to_))["h_bins"]

    with open(fout_, "w") as fd:

        for _from, _to in date_couple:

            print(_from, _to, end="\r")
            from_ts, to_ts = _from.timestamp(), _to.timestamp()
            # faire l'appel à l'API, renvois un dictionnaire
            _tmp = sess.get_coin_market_chart_range_by_id(
                id_, vs_currency_, from_ts, to_ts
            )
            # change the dictionnary returned by the API in a DataFrame
            _tmp = market_chart_range_to_df(_tmp)
            # et concatène le résultat dans une grande dataFrame
            if df is None:
                df = _tmp
                header = True
            else:
                df = concat([df, _tmp], axis=0)
                header = False

            sleep(pause_)

            # finaly write down the results
            # try to do that without loading the memory.

            df.to_csv(fd, header=header)
    logger.warning(f"data in {fout_}")
    return df


def parse_args():
    """Settings the applications's arguments and options."""
    description = """An application to download bitmex's data with what ever resolution you need."""
    fout_dft = "cg-data"
    fout_help = (
        f"base Name of the csv file where to save the results. (default {fout_dft}.csv)"
    )
    id_token_dft = "cardano"
    id_token_help = f"token name for wich we get the name. (default {id_token_dft})"
    vs_currency_dft = "usd"
    vs_currency_help = f"base Name of the currency in wich to expres the token's value (default {vs_currency_dft})"
    pause_dft = 1.2
    pause_help = f"Min time to wait between 2 requests (default {pause_dft}).  to avoid overloading the server.  Coingecko limites to 600 req/m"
    startTime_dft = "2020-01-01"
    startTime_help = f"Time to start the data collection (default, {startTime_dft}).  Check time zones"
    endTime_dft = "2020-02-01"
    endTime_help = (
        f"Time to end the data collection (default, {endTime_dft})).  Check TZ"
    )
    logLevel_dft = "WARNING"
    logLevel_help = "set the log level"

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--fout", "-f", help=fout_help, default=fout_dft)
    parser.add_argument("--pause", "-p", type=float, help=pause_help, default=pause_dft)
    parser.add_argument("--startTime", "-s", help=startTime_help, default=startTime_dft)
    parser.add_argument("--endTime", "-e", help=endTime_help, default=endTime_dft)
    parser.add_argument("--logLevel", "-L", help=logLevel_help, default=logLevel_dft)
    parser.add_argument(
        "--vs_currency", "-c", help=vs_currency_help, default=vs_currency_dft
    )
    parser.add_argument("--id_token", "-t", help=id_token_help, default=id_token_dft)

    return parser.parse_args()
p

def main_prg():
    """Run the main programme."""
    args = parse_args()

    logger.setLevel(args.logLevel)

    startTime = Timestamp(args.startTime).tz_localize(OS_TZ)
    endTime = Timestamp(args.endTime).tz_localize(OS_TZ)

    query = {
        "from_": startTime,
        "to_": endTime,
        "pause_": args.pause,
        "id_": args.id_token,
        "vs_currency_": args.vs_currency,
        "fout_": args.fout,
    }

    _ = getcg_market_trades(**query)
    return None


if __name__ == "__main__":
    main_prg()
    sys.exit()
