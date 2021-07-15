# -*- coding: utf-8 -*-
# ~/Python/Env_sys/KolaVizPrj/KolaViz/Lib/fnames.py voir
from time import sleep
from typing import Union, Sequence, Dict, Optional
from pathlib import Path
import os
from os.path import getmtime

from numpy import array
from pandas import (
    concat,
    MultiIndex,
    DataFrame,
    Series,
    Index,
    to_datetime,
    Timestamp,
    Timedelta,
)
from pycoingecko.api import CoinGeckoAPI

from Sources.cg_logging import logger  #
from Sources.cg_times import _now, now_as_ts, coerce_from_tsh_to_int  #
from Sources.cg_settings import APISLEEP, DATEGENESIS, DFT_OLDAGE  #
from Sources.cg_exceptions import (
    LenHomogeneousException,
    TypeHomogeneousException,
    ShapeHomogeneousException,
)  #

from Sources.cg_io import read_csv  # log
from Sources.cg_decorators import w_retry, as_pd_object  # log and set

"""cg_lib.py: Fonctions pour faciliter l'accès au données pour leur formattage"""


def get_historical_capitalisation_by_id(
    cg: CoinGeckoAPI,
    id_: str = "cardano",
    vs_currency="btc",
    from_ts=Timestamp("2008-01-01"),
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
        to_ts = Timestamp.now()

    from_ts, to_ts = coerce_from_tsh_to_int([from_ts, to_ts])

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
@as_pd_object("Series")
def w_get_coin_by_id(_id, **kwargs):
    """Juste a easy wrapper around the standard api function"""
    return cg.get_coin_by_id(_id, **kwargs)


@w_retry()
def w_get_coin_market_chart_range_by_id(
    cg: CoinGeckoAPI,
    id_: str = "cardano",
    vs_currency="btc",
    from_ts=DATEGENESIS,
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
        to_ts = _now()
    from_ts, to_ts = coerce_from_tsh_to_int([from_ts, to_ts])

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
    from_ts=DATEGENESIS,
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
        to_ts = _now()
    from_ts, to_ts = coerce_from_tsh_to_int([from_ts, to_ts])

    _data = cg.get_coin_market_chart_range_by_id(
        id=id_, vs_currency=vs_currency, from_timestamp=from_ts, to_timestamp=to_ts,
    )
    sleep(APISLEEP())
    try:
        return convert_dict_to_df(_data, ts_index=True)
    except AssertionError:
        return DataFrame(_data)


def retry(func, *args, **kwargs):
    attempts = 0
    while attempts < 20:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            attempts += 1
            sleep(APISLEEP())
            logger.info(
                f"{_now()}: Failed {attempts}: {e.__str__()} with {args}. Trying Again !"
            )

    return None


def get_file_age(
    _file: Union[str, Path], _now: Optional[Timestamp] = None
) -> Timedelta:
    """Return the age of a faile relative to _now"""
    if _now is None:
        return now_as_ts() - Timestamp(getmtime(_file), unit="s").round("s")
    else:
        return _now - Timestamp(getmtime(_file), unit="s").round("s")


def is_old(_file: Union[str, Path], age: Timedelta = DFT_OLDAGE):
    """Return true if file is older than age"""
    return Timestamp(getmtime(_file), unit="s") < (now_as_ts() - age)


def is_between(x, a, b, strict="yes") -> bool:
    """test if x is in (a,b)"""
    if strict == "yes":
        return bool(a < x and x < b)
    if strict == "no":
        return bool(a <= x and x <= b)
    if strict == "left":
        return bool(a < x and x <= b)
    if strict == "right":
        return bool(a <= x and x < b)
    raise Exception("Shoul have an answer here")


def check_mode(mode: str, file_ext: str) -> bool:
    """Check that I have a correct mode for my application.
    should be x w a+ optionnaly with b fileextention is pkl"""

    def err_msg():
        return f"mode={mode} and file_ext={file_ext}"

    assert len(mode) <= 3, err_msg()

    if file_ext == ".pkl":
        assert "b" in mode, err_msg()
    # if "+" in mode:
    #     assert "w" in mode, err_msg()
    return True


def _is_empty(x: Sequence) -> bool:
    try:
        return len(x) == 0
    except TypeError:
        # excpet TypeError: object of type '....' has no len()
        # no len == is empty
        return True


def _is_constant(x: Union[int, float, str]) -> bool:
    return isinstance(x, (int, float, str))


def _is_none(x) -> bool:
    return x is None


def are_valide_coin_ids(cg: CoinGeckoAPI, ids: Sequence) -> bool:
    """check that ids are coins ids"""
    coin_ids = get_coins_list(cg, update_local=False)
    left_ids = set(ids) - set(coin_ids)
    assert not len(left_ids), f"left_ids={left_ids} for ids={ids}"
    return True


def filtre(df: DataFrame, where: str, col) -> DataFrame:
    """
    Filtre un dataframe avec la condition where
    """
    return DataFrame(df.where(df.loc[:, col].str.contains(where)).dropna())


# def expand(tuple_, list_) -> List:
#     """Get le support de tuple dans list"""
#     assert tuple_ == sorted(tuple_)
#     _list = sorted(list_)
#     return [e for e in _list if is_between(e, *tuple_, strict="no")]


def dict_value_filter(dict_: dict, criteria) -> dict:
    """
    Return the dict_ for which the values match criteria.

    criteria is a function returning a boolean to apply to a each values
    """
    return {k: v for (k, v) in dict_.items() if criteria(v)}


def harmonise_dict_of_list(dict_: dict, as_df: bool = True):
    """
    Make sure the dictionnary with different lenght of list
    is converterd to dataframe requireing same length
    dict_
    The problem is that the dict that we need to converte are sometime of
    different length
    """
    assert set(dict_.keys()) == set(["prices", "market_caps", "total_volumes"])

    entries = Series(dict_).apply(len)
    sorted_entries = entries.sort_values(axis=0)
    biggest_index_label = sorted_entries.index[-1]
    biggest_index_ts = array(dict_[biggest_index_label])[:, 0]

    _df = DataFrame(index=biggest_index_ts)

    for k in dict_.keys():
        _tmp = DataFrame(dict_[k]).set_index(0)
        _tmp.columns = [k]
        _df = _df.merge(_tmp, how="outer", left_index=True, right_index=True)

    if as_df:
        _df.index.name = "ts"
        _df.index = to_datetime(_df.index.values * 1e6)
        return _df
    else:
        return {k: array(list(v.items())) for (k, v) in _df.items()}


def dict_homogenous(dict_: dict, taille=None) -> bool:
    """Return true if the entries of the dict are of same type and shape"""
    # need to catch the assertion error or raise a specific error type

    assert isinstance(dict_, dict)

    if len(dict_) == 0:
        return True

    if dict_test_entries(dict_) == len(dict_):
        return True

    assert dict_test_entries(dict_) == 0

    _k0, _v0 = list(dict_.items())[0]

    def _array_of(func):
        return {k: func(v) for (k, v) in dict_.items()}

    def _test(func):
        return all([func(_v0) for v in dict_.values()])

    def _type(v):
        return isinstance(v, type(_v0))

    def _len(v):
        return len(v) == len(_v0)

    def _shape(v):
        return array(v).shape[1] == taille

    if not _test(_type):
        # array_type = {k: type(v) for (k, v) in dict_.items()}
        raise TypeHomogeneousException(
            f"All values shoudl be of TYPE {type(_v0)} but {_array_of(lambda v: type(v))}"
        )

    if not _test(_len):
        # array_len = {k: len(v) for (k, v) in dict_.items()}
        raise LenHomogeneousException(
            f"All values should have LEN {len(_v0)} but  {_array_of(lambda v: len(v))}"
        )

    if taille is not None:
        if not _test(_shape):
            # array_shape = {k: array(v).shape for (k, v) in dict_.items()}
            raise ShapeHomogeneousException(
                f"All values should have SHAPE {taille} but {_array_of(lambda v: array(v).shape)}."
            )

    return True


def dict_test_entries(dict_: dict, test: str = "none-empty-const") -> int:
    """
    Return the number of empty values or none value in the dict_
    tests should be a string with kewords specifying which test
    to carry: 'none', 'empty', 'const'
    default 'none-empty-const'
    """
    assert len(dict_) != 0, "We test non empty dictionaries"
    assert (
        "none" in test or "empty" in test or "const" in test
    ), f"test should containe 'none, cont or empty but it's {test}"

    sum_of_none_empty_const_entries = 0
    if "none" in test:
        sum_of_none_empty_const_entries += len(dict_value_filter(dict_, _is_none))
    if "empty" in test:
        sum_of_none_empty_const_entries += len(dict_value_filter(dict_, _is_empty))
    # if "const" in test:
    #     sum_of_none_empty_const_entries += len(dict_value_filter(dict_, _is_constant))

    return sum_of_none_empty_const_entries


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
