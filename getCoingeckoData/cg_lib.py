# -*- coding: utf-8 -*-
# ~/Python/Env_sys/KolaVizPrj/KolaViz/Lib/fnames.py voir
from time import sleep
from typing import Union, Sequence

from pandas import DataFrame
from pycoingecko.api import CoinGeckoAPI


from cg_times import _now  #
from cg_settings import APISLEEP  #
from cg_logging import logger  #

from cg_api import get_coins_list

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
