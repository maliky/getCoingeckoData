# -*- coding: utf-8 -*-
"""
cg_exceptions.py
"""


class TypeHomogeneousException(Exception):
    def __init__(self, message, load=None, extra=None):
        super().__init__(message, load, extra)


class LenHomogeneousException(Exception):
    def __init__(self, message, load=None, extra=None):
        super().__init__(message, load, extra)


class ShapeHomogeneousException(Exception):
    def __init__(self, message, load=None, extra=None):
        super().__init__(message, load, extra)
