from contextlib import contextmanager
import datetime as dt
import os

import pandas as pd  # type: ignore
import numpy as np  # type: ignore

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
def clip_srs(
    s, clip_percentile=99.9, ignore_nulls=True, val=None, vb=False
):
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
