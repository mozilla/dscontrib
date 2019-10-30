import datetime as dt

import pandas as pd
import numpy as np

"""
* clip

"""

# Help with dates
SUB_FMT = "%Y%m%d"
SUB_FMT_DASH = "%Y-%m-%d"
SUB_FMT_SPK = "yyyyMMdd"


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


def s3(date):
    return date.strftime(SUB_FMT)


def s3_dash(date):
    return date.strftime(SUB_FMT_DASH)


# Stats
def clip_srs(s, clip_percentile=99.9, ignore_nulls=True, vb=False):
    if ignore_nulls:
        val = np.percentile(s[s == s], clip_percentile)
    else:
        val = np.percentile(s, clip_percentile)
    if vb:
        print("{} -> {}".format(s.max(), val))
    sc = np.clip(s, None, val)
    return sc
