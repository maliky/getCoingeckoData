# -*- coding: utf-8 -*-

from pathlib import Path
import os
from time import sleep
from math import floor

from pandas import DataFrame, Series, Timestamp

from pycoingecko.api import CoinGeckoAPI

from cg_logging import logger  #
from cg_settings import APISLEEP, DATEGENESIS  #
from cg_times import _now  #

from cg_io import read_csv  # log
from cg_decorators import w_retry, as_pd_object  # log and set
from cg_lib import convert_dict_to_df  

"""Direct calls to the Coingecko API"""


def get_historical_capitalisation_by_id(
    cg: CoinGeckoAPI,
    id_: str = "cardano",
    vs_currency="btc",
    from_ts=int(Timestamp("2008-01-01").timestamp()),
    to_ts=None,
    to_td_=None,
) -> DataFrame:
    """
    get the capitalisation historical data for a specific range
    from_ts : when to start
    to_ts: when to stop
    to_td: how long to get from the from_ts

    return: a df
    """
    # assert to_ts is None and to_td_ is None, f"choose which to setup  to_s or to_td_ ?"

    if to_ts is None:
        # set it to now
        # _to_td = timedelta(1).total_seconds() if to_td_ is None else to_td_
        to_ts = floor(Timestamp.now().timestamp())

    # import ipdb; ipdb.set_trace()

    _data = cg.get_coin_market_chart_range_by_id(
        id=id_, vs_currency=vs_currency, from_timestamp=from_ts, to_timestamp=to_ts,
    )
    return convert_dict_to_df(_data, ts_index=True)


@w_retry()
@as_pd_object("DataFrame")
def w_get_coins_list(cg: CoinGeckoAPI) -> DataFrame:
    """Juste a easy wrapper around the standar api function"""
    return DataFrame(cg.get_coins_list())


def get_coins_list(
    cg: CoinGeckoAPI,
    token_list_fn: Path = Path("./data/simple_token_list.csv"),
    update_local: bool = True,
    simple: bool = True,
) -> Series:
    """
    check if the folder/coin_list existe and if not fall back on an api call and populate it
    if update_local we update the list with latest downloaded
    is simple then keep 'true' tokens not the long
    if update local, the token_list should be accessible
    """

    def _logging_update(verb, seta, setb):
        coins_diff = set(seta) - set(setb)
        logger.info(f"{verb} {token_list_fn} {len(coins_diff)} coins.")

    coin_list = w_get_coins_list(cg, as_df=True)
    if simple:
        coin_list = coin_list.where(
            coin_list.id.str.lower() == coin_list.name.str.lower()
        ).dropna()

    if update_local:

        assert os.path.exists(token_list_fn), f"{token_list_fn} not accessible"
        coin_list_id = read_csv(token_list_fn, index_col=0).id

        # logging some infos
        if len(coin_list.id) > len(coin_list_id):
            _logging_update("Adding to ", coin_list.id, coin_list_id)
        elif len(coin_list.id) < len(coin_list_id):
            _logging_update("Removing from ", coin_list_id, coin_list.id)

        coin_list.to_csv(token_list_fn)

    return Series(coin_list.id)


@w_retry()
def w_get_coin_market_chart_range_by_id(
    cg: CoinGeckoAPI,
    id_: str = "cardano",
    vs_currency="btc",
    from_ts=int(DATEGENESIS.timestamp()),
    to_ts=None,
    to_td_=None,
) -> DataFrame:
    """
    get the capitalisation historical data for a specific range
    from_ts : when to start
    to_ts: when to stop
    to_td: how long to get from the from_ts

    return: a df
    """
    if to_ts is None:
        # set it to now
        # _to_td = timedelta(1).total_seconds() if to_td_ is None else to_td_
        to_ts = _now()

    _data = cg.get_coin_market_chart_range_by_id(
        id=id_, vs_currency=vs_currency, from_timestamp=from_ts, to_timestamp=to_ts,
    )
    sleep(APISLEEP())
    try:
        return convert_dict_to_df(_data, ts_index=True)
    except AssertionError:
        return DataFrame(_data)


def _get_coin_market_chart_range_by_id(
    cg: CoinGeckoAPI,
    id_: str = "cardano",
    vs_currency="btc",
    from_ts=int(DATEGENESIS.timestamp()),
    to_ts=None,
    to_td_=None,
) -> DataFrame:
    """
    get the capitalisation historical data for a specific range
    from_ts : when to start
    to_ts: when to stop
    to_td: how long to get from the from_ts

    return: a df
    """
    if to_ts is None:
        # set it to now
        # _to_td = timedelta(1).total_seconds() if to_td_ is None else to_td_
        to_ts = _now()
    _data = cg.get_coin_market_chart_range_by_id(
        id=id_, vs_currency=vs_currency, from_timestamp=from_ts, to_timestamp=to_ts,
    )
    sleep(APISLEEP())
    try:
        return convert_dict_to_df(_data, ts_index=True)
    except AssertionError:
        return DataFrame(_data)
