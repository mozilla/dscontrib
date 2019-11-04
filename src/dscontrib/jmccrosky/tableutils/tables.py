# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import pandas as pd
import numpy as np
import scipy.stats as stats
import statsmodels.stats.proportion as stp
from itertools import product
from tabulate import tabulate


def medianCI(data, ci, p):
    if type(data) is pd.Series or type(data) is pd.DataFrame:
        data = data.values

    data = data.reshape(-1)
    data = np.sort(data)
    N = data.shape[0]

    lowCount, upCount = stats.binom.interval(ci, N, p, loc=0)
    lowCount -= 1
    return data[int(lowCount)], data[int(upCount)]


def t_ci(x):
    df = len(x)-1
    scale = np.std(x) / np.sqrt(len(x))
    return stats.t.interval(alpha=0.95, df=df, loc=np.mean(x), scale=scale)


def mean_with_ci(x):
    return "{:.2f} ({:.2f} - {:.2f})".format(np.mean(x), t_ci(x)[0], t_ci(x)[1])


def median_with_ci(x):
    return "{:.2f} ({:.2f} - {:.2f})".format(
        np.median(x), medianCI(x, 0.95, 0.5)[0], medianCI(x, 0.95, 0.5)[1]
    )


def prop_greater_with_ci(x, thresh):
    num = np.sum(x >= thresh)
    den = float(len(x))
    ci = stp.proportion_confint(num, den)
    return "{:.2f} ({:.2f} - {:.2f})".format(num/den, ci[0], ci[1])


def gen_prop_greater_with_ci(thresh):
    return lambda x: prop_greater_with_ci(x, thresh)


class Metric:
    def __init__(self, name, function):
        self.name = name
        self.function = function


class SliceSpec:
    def __init__(self, slices={"All": lambda x: x}, slice_order=["All"]):
        self.slices = slices
        self.slice_order = slice_order

    @classmethod
    def complete_dimension(cls, data, var):
        return cls(
              {
                    "{}={}".format(var, val):
                    (lambda x, val=val: x.query("{}=='{}'".format(var, val)))
                    for val in data[var].unique()
              },
              ["{}={}".format(var, val) for val in sorted(data[var].unique())]
        )


def summary_table(data, vars, metrics, slice_spec=SliceSpec(), filter=None):
    table_data = [
        ["Variable"] + vars,
    ]
    table_data = table_data + [
        ["{} ({})".format(metric.name, slice)] + [
            (
                metric.function(
                    (
                        slice_spec.slices[slice](
                            data.dropna(subset=[var]).query(
                                filter.format(var=var)
                            )
                        )
                    )[var] if filter is not None else (
                        slice_spec.slices[slice](data.dropna(subset=[var]))
                    )[var]
                )
            ) for var in vars
        ]
        for (metric, slice) in product(metrics, slice_spec.slice_order)
    ]
    return tabulate(list(map(list, zip(*table_data))), headers="firstrow")
