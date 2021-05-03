#!/usr/bin/env python
# coding: utf-8
from sys import exit
from typing import Dict

from pandas import concat, MultiIndex

from cg_logger import logger
from cg_io import load_data, save_data
"""cg_formating.py: Format capitalisation data pre-saved on disk"""


def format_data(D: Dict, logLevel=None):
    """Enlève les colonnes non nécessaires"""
    if logLevel is not None:
        getattr(logger, logLevel)(f"Format data for {len(D)} objects in {type(D)}")

    E = {}
    for i, coinid in enumerate(D):
        print(f"{i}/{len(D)} trimming...", end="\r")
        try:
            if len(D[coinid]):
                E[coinid] = D[coinid].drop(["total_volumes", "prices"], axis=1)
                E[coinid].columns = MultiIndex.from_product(
                    [[coinid], E[coinid].columns]
                )
                E[coinid] = E[coinid].droplevel(1, axis=1)
                E[coinid].columns.name = "market_caps"

        except Exception as e:
            print(coinid)
            raise e

    return E


def main():
    """main programme"""
    D = load_data("./data/all-historical-cap.pkl", logLevel="INFO")

    E = format_data(D, logLevel="INFO")

    logger.info("Merging big df")
    J = concat([df for df in list(E.values())], axis=1)

    save_data(J, fileout="./data/all-hc-df.pkl", logLevel="INFO")

    logger.info("Done !")
    exit


if __name__ == "__main__":
    main()
