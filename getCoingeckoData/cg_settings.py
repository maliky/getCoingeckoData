# -*- coding: utf-8 -*-
"""
Config string for coingecko download utilities
"""
from pandas import Timestamp, Timedelta
import random

DATEGENESIS = Timestamp("2009-01-01")
DFT_OLDAGE = Timedelta("23h")


def APISLEEP() -> float:
    """returning a random float with mean 1.2"""
    return 0.7 + random.betavariate(2, 2)
