#!/usr/bin/env python
# coding: utf-8
from pandas import concat

from Sources.cg_logging import logger  #

from Sources.cg_io import load_data, save_data  # log
from Sources.cg_lib import format_data


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
