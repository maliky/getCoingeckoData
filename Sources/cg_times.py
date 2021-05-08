# -*- coding: utf-8 -*-
from typing import Union, Optional, Sequence
from pandas import Timestamp, Series, DataFrame, date_range, Timedelta
from math import floor


"""cg_times.py  : Utilities working with time object"""


def get_recent_data(df: DataFrame, delta=Timedelta(30, "d")) -> DataFrame:
    """On suppose que l'index est un timestmap index"""
    most_recent_date = df.index[-1]
    return DataFrame(df.loc[(most_recent_date - delta) :])


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

    def _to_ts(ts: Timestamp) -> int:
        return int(ts.timestamp() * 10 ** 6)

    ts_range = list(map(_to_ts, tsh_range))

    return {
        "bins": list(zip(ts_range[:-1], ts_range[1:])),
        "h_bins": list(zip(tsh_range[:-1], tsh_range[1:])),
        "h_range": tsh_range,
        "range": ts_range,
    }


def now_as_float(_round: str = "s") -> float:
    """Return the time now, rounded as a float"""
    _now_ = Timestamp.now()
    if round is not None:
        _now_ = _now_.round(_round)

    return floor(_now_.timestamp())


def now_as_ts(_round: str = "s") -> Timestamp:
    """Return the time now, rounded as a Timestamp"""
    return Timestamp(_now(as_ts=False, _round=_round))


def _now(as_ts: bool = False, _round: str = "s") -> Union[float, Timestamp]:
    """Shortcut to return now.  set round to None to avoid rounding."""
    _now_ = Timestamp.now()
    if round is not None:
        _now_ = _now_.round(_round)

    return floor(_now_.timestamp()) if as_ts else _now_


def coerce_from_tsh_to_int(tss: Sequence) -> Sequence:
    """Coerce the sequence of ts in int format"""

    def _to_ts(ts):
        if isinstance(ts, Timestamp):
            return int(ts.timestamp())
        return ts

    return list(map(_to_ts, tss))


def ts_extent(ref_: Union[Series, DataFrame], as_unix_ts_=False):
    """
    Return extrems of a iterable.
    if a dataframe is passed to ref_ return the extent of its index
    if as_unix_ts is True, suppors the iterable avec a timestamp method
    """
    if len(ref_) == 0:
        return None, None

    _idx = ref_.index if isinstance(ref_, (DataFrame, Series)) else ref_

    tsh = _idx[0], _idx[-1]
    try:
        tsh_converted = None
        _idx[0].timestamp()
    except Exception:
        # raise f"tsh ={tsh} should be timestamp"
        def _to_ts(t):
            return Timestamp(t * 1e9)

        tsh_converted = list(map(_to_ts, tsh))

    if not as_unix_ts_:
        return tsh if tsh_converted is None else tsh_converted
    else:
        # assert
        return list(
            map(
                lambda x: int(x.timestamp()),
                tsh if tsh_converted is None else tsh_converted,
            )
        )
