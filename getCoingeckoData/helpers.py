# -*- coding: utf-8 -*-
from pandas import DataFrame, Timestamp
from datetime import timedelta
from mlkHelper.dataAnalysis import convert_dict_to_df
from pycoingecko.api import CoinGeckoAPI
from math import floor
"""
Fonctions pour faciliter l'accès au données
"""


def filtre(df: DataFrame, where: str, col) -> DataFrame:
    """
    Filtre un dataframe avec la condition where
    """
    return df.where(df.loc[:, col].str.contains(where)).dropna()


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
