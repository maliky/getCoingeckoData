#!/usr/bin/env python
# coding: utf-8
from sys import exit
from typing import Dict
from numpy import array

from pandas import concat, MultiIndex, DataFrame, Series, Index, to_datetime

from cg_logging import logger  #
from cg_exceptions import (
    LenHomogeneousException,
    TypeHomogeneousException,
    ShapeHomogeneousException,
)  #
from cg_io import load_data, save_data  # log
from cg_lib import _is_none, _is_empty

"""cg_formating.py: Format capitalisation data pre-saved on disk"""


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
