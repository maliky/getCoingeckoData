# -*- coding: utf-8 -*-
# ~/Python/Env_sys/KolaVizPrj/KolaViz/Lib/fnames.py voir
from time import sleep
from typing import Union, Sequence
from math import floor

from numpy import array
from pandas import DataFrame, concat, Series, Index, to_datetime, Timestamp
from pycoingecko.api import CoinGeckoAPI

from cg_api import get_coins_list
from cg_exceptions import (
    LenHomogeneousException,
    TypeHomogeneousException,
    ShapeHomogeneousException,
)


from cg_times import _now
from cg_settings import APISLEEP
from cg_logging import logger

"""Fonctions pour faciliter l'accès au données"""


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


def dict_value_filter(dict_: dict, criteria) -> dict:
    """
    Return the dict_ for which the values match criteria.

    criteria is a function returning a boolean to apply to a each values
    """
    return {k: v for (k, v) in dict_.items() if criteria(v)}


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
    if "a" in mode:
        assert "+" in mode, err_msg()
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


def harmonise_dict_of_list(dict_: dict, as_df: bool = True):
    """
    Make sure the dictionnary with different lenght of list
    is converterd to dataframe requireing same length
    dict_
    The problem is that the dict that we need to converte are sometime of
    different length
    """
    assert set(dict_.keys()) == set(["prices", "market_caps", "total_volumes"])

    biggest_index_label = Series(dict_).apply(len).sort_values().index[-1]
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


# def expand(tuple_, list_) -> List:
#     """Get le support de tuple dans list"""
#     assert tuple_ == sorted(tuple_)
#     _list = sorted(list_)
#     return [e for e in _list if is_between(e, *tuple_, strict="no")]


# def coin_ids_to_filenames(cg: CoinGeckoAPI, args_coins, folder: str, file_ext: str) -> List:
#     """Parse the list of coins and return a set of filenames to processe"""
#     # should I make a specific currency folder?
#     arg_coins = parse_coin_ids(cg, args_coins)
#     return [Path(folder).joinpath(f"{coin}{file_ext}") for coin in arg_coins]


# def parse_coin_ids(cg: CoinGeckoAPI, args_coins: str = "a-fullname"):
#     """
#     get a string specifing how to parse coins.
#     [sort-order][size]-[sort-key] with sort-order 'a' or 'd' (def. 'a')
#     a size is the number of records to return. if none, return all (def all)
#     a sort-key in 'mtime', 'size', 'name' (def. name)
#     can also be juste coins ids separated by commas

#     returns the ids of the coins
#     """
#     # get all possible coins_ids from API
#     coin_ids = get_coins_list(cg, update_local=False)

#     ret_id = []
#     for args in args_coins.split(","):
#         value = args.split("-")
#         assert len(value) in [
#             1,
#             2,
#         ], f"value={value}, args_coins={args_coins}, args={args}"

#         ret_id += expand(value, coin_ids) if len(value) == 2 else value

#     assert are_valide_coin_ids(
#         cg, ret_id
#     ), f"{args_coins} and {coin_ids}, diff {set(args_coins) - set(coin_ids)}"

#     return ret_id
