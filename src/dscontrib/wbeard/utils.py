from contextlib import contextmanager
import datetime as dt
from functools import wraps
import os

import pandas as pd
import numpy as np

"""
* clip

"""

# Help with dates
SUB_FMT = "%Y%m%d"
SUB_FMT_DASH = "%Y-%m-%d"
SUB_FMT_SPK = "yyyyMMdd"


def lmap(f, xs):
    return [f(x) for x in xs]


class Date:
    def __init__(self, date_str):
        self.date_str = date_str
        self.d = d = pd.to_datetime(date_str)
        self.s = d.strftime(SUB_FMT)
        self.sd = d.strftime(SUB_FMT_DASH)

    def __repr__(self):
        return "Date({})".format(self.sd)

    def add_days(self, n_days):
        return Date(add_days(n_days, self.d))

    def __eq__(self, o):
        return self.d.date() == o.d.date()

    def __sub__(self, o):
        return (self.d - o.d).days

    @staticmethod
    def today():
        return Date(dt.date.today())


def add_days(n, date):
    ni = int(n)
    if type(ni) != type(n):
        print("Warning, type converted")
    ret = date + dt.timedelta(days=ni)
    return ret


def round_week(d):
    d = pd.to_datetime(d.dt.date)
    dow = pd.to_timedelta(d.dt.dayofweek, unit="d")
    return d - dow


def s3(date):
    return date.strftime(SUB_FMT)


def s3_dash(date):
    return date.strftime(SUB_FMT_DASH)


# Stats
def clip_srs(s, clip_percentile=99.9, ignore_nulls=True, val=None, vb=False):
    if val is None:
        if ignore_nulls:
            val = np.percentile(s[s == s], clip_percentile)
        else:
            val = np.percentile(s, clip_percentile)
    if vb:
        print("{} -> {}".format(s.max(), val))
    sc = np.clip(s, None, val)
    return sc


@contextmanager
def working_directory(path):
    """
    A context manager which changes the working directory to the given
    path, and then changes it back to its previous value on exit.

    Usage:
    > # Do something in original directory
    > with working_directory('/my/new/path'):
    >     # Do something in new directory
    > # Back to old directory
    """

    prev_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


# Pandas
def requires_cols(requires, assert_new=None, keep_extra=True, verbose=False):
    """
    Decorator to document and ensure that a function
    requires the DataFrame (which is the first argument)
    to have columns `requires`.
    To ensure
    The following function

    @requires_cols(['a', 'b'])
    def add_a_b(df):
        return df.a + df.b

    will raise a KeyError on the following input:
    >>> add_a_b(DataFrame(dict(a=[1, 2], c=[10, 10])))
    but not on the following
    >>> add_a_b(DataFrame(dict(a=[1, 2], b=[10, 10])))

    This mainly helps document the required columns that
    a DataFrame will need, and enforces the documentation.

    If `assert_new` is a sequence of columns, the decorator
    ensures that the function creates these new columns exactly.
    E.g.,

    @requires_cols(['a', 'b'], assert_new=['c'])
    def add_a_b(df):
        df['c'] = df.a + df.b
        return df
    """
    assert_new = set(assert_new or [])

    def deco(f):
        @wraps(f)
        def wrapper(df, **kw):
            orig_cols = set(df)
            _df = df
            df = df[requires].copy()
            tmp_missing_cols = set(df) - orig_cols

            res_df = f(df, **kw)

            if assert_new:
                assert isinstance(
                    res_df, pd.DataFrame
                ), f"result is a {type(res_df)}, not a DataFrame"
                new_cols = set(res_df) - set(requires)
                assert new_cols == assert_new, f"{new_cols} != {assert_new}"

            if verbose:
                s2 = set(res_df)
                print(f"+ {sorted(s2 - set(requires))}")
                print(f"- {sorted(set(requires) - s2)}")
            if keep_extra:
                for c in tmp_missing_cols:
                    res_df[c] = _df[c]
            return res_df

        return wrapper

    return deco
