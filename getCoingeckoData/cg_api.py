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


"""Direct calls to the Coingecko API"""

def convert_dict_to_df(dict_: dict, ts_index: bool = True, taille=None) -> DataFrame:
    """
    Convertis un dictionnaire dont les entrées ont toutes la même shape et dont
    la première ligne est un ts.
    """
    try:

        dict_homogenous(dict_, taille=taille)
    except AssertionError as ae:
        raise ae
    except TypeHomogeneousException as the:
        raise the
    except LenHomogeneousException as lhe:
        logger.exception(f"Handling {lhe}")
        dict_ = harmonise_dict_of_list(dict_, as_df=False)
        pass
    except ShapeHomogeneousException as she:
        raise she

    # if we have empty or none entries
    if dict_test_entries(dict_, test="empty-none") != 0:
        return DataFrame(None)

    if dict_test_entries(dict_, test="const") != 0:
        return DataFrame(Series(dict_))

    df = DataFrame(None)
    first_pass = True

    for k in dict_:
        _data = array(dict_[k]).T
        if first_pass:
            # Initialisation avec  ts index
            df = DataFrame(index=_data[0], data=_data[1], columns=[k])
            first_pass = False
        else:
            df = concat([df, Series(index=_data[0], data=_data[1], name=k)], axis=1)

    def _to_ts_dt(ts):
        return ts.round("s")

    if ts_index:
        _index = Index(map(_to_ts_dt, to_datetime(df.index.values * 1e6)), name="ts")
        df = df.set_index(_index)

    df.columns.name = "---"
    return df


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
