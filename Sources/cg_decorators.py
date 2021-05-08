# -*- coding: utf-8 -*-
from time import sleep
from collections import OrderedDict
from pandas import DataFrame, Series

from inspect import signature, Parameter, functools
from functools import wraps

from Sources.cg_settings import APISLEEP  #
from Sources.cg_logging import logger  #

"""cg_decorators.py"""


def w_retry(max_attemps: int = 10, sleep_time: float = APISLEEP()):
    """Return a wrapper to handle network communication errors"""

    def handle_attemps(error_, attemps_, args_, kwargs_, sleep_time_):
        """factorise attemps handling"""
        _attemps = attemps_ + 1
        logger.exception(
            f"Failed {_attemps}: '{error_}'. args={args_} kwargs={kwargs_}."
            f" Sleeping {sleep_time_ ** _attemps}s and trying Again !"
        )
        sleep(sleep_time_ ** _attemps)
        return _attemps

    def wrapper(func):
        def wrapped_func(*args, **kwargs):
            attemps = 0
            while attemps < max_attemps:
                try:
                    return func(*args, **kwargs)
                except ValueError as ve:
                    # handling the case where the coin is not present
                    if "not find coin" in str(ve):
                        logger.exception(f"PASSING (ignoring)  ValueError={ve}")
                        return None
                    attemps = handle_attemps(ve, attemps, args, kwargs, sleep_time)
                except Exception as e:
                    # other errors
                    attemps = handle_attemps(e, attemps, args, kwargs, sleep_time)
            raise Exception(f"Retried {attemps} times but Failled")

        return wrapped_func

    return wrapper


def as_pd_object(_type: str):
    """
    Add an argument to the function to format the ouput in a pd.objet DataFrame or Series
    """
    # TODO manage to add documentation for the added argument
    arg_name = {"DataFrame": "as_df", "Series": "as_series"}[_type]

    def add_doc_and_arg_to(func):
        """
        add a specific argument to the func's signature
        and add a ligne in documentation
        """
        func_sig = signature(func)
        func_sig_params = OrderedDict(func_sig.parameters).copy()
        if arg_name in func_sig_params:
            logger.warning(
                f"We are modifing {arg_name}, an existing argument of func={func}."
            )

        func_sig_params[arg_name] = Parameter(
            name=arg_name,
            kind=Parameter.POSITIONAL_OR_KEYWORD,
            default=False,
            annotation="bool",
        )

        # func_sig.replace(parameters=func_sig_params.values())
        func.__doc__ += f"\n-{arg_name} permet de sortir le résultat comme un {_type}"
        return func

    def wrapped_f(func):

        assert _type in ["DataFrame", "Series"], f"_type={_type} for f={func}"

        if _type == "DataFrame":

            @wraps(func)
            def new_df_f(*args, as_df=False, **kwgs):
                inner = func(*args, **kwgs)
                try:
                    return DataFrame(inner) if as_df else inner
                except ValueError as ve:
                    if "index" in ve.__str__():
                        # probably that we only have one column of data
                        # passing it to a serie first to automaticaly generate the index
                        return DataFrame(Series(inner))
                    raise ve

            new_df_f = add_doc_and_arg_to(new_df_f)

            return new_df_f

        if _type == "Series":

            @wraps(func)
            def new_s_f(*args, as_series=False, **kwgs):
                inner = func(*args, **kwgs)
                return Series(inner) if as_series else inner

            new_s_f = add_doc_and_arg_to(new_s_f)
            return new_s_f

    return wrapped_f
