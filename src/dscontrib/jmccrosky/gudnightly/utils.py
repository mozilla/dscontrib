# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pandas as pd
import numpy as np

from pyspark.sql import functions as F
from pyspark.sql import Window
from astropy.stats import jackknife_stats


def calculateDateWindow(
        plot_start_date, plot_end_date, smoothing, comparison_mode, pre_days, post_days
):
    start_window = plot_start_date - \
        pd.DateOffset(days=smoothing-1) - \
        pd.DateOffset(days=pre_days) - \
        pd.to_timedelta('1 second')           # Needed due to pyspark between() bug
    end_window = plot_end_date + \
        pd.to_timedelta('1 second') + \
        pd.DateOffset(days=post_days)
    window = [(start_window, end_window)]
    if comparison_mode in ["YoY", "Last Year"]:
        window = window + [(
            start_window - pd.DateOffset(years=1),
            end_window - pd.DateOffset(years=1)
        )]
    return window


def doSmoothing(data, usage_criteria, dimension_cols, smoothing_lookback):
    windowSpec = Window.partitionBy(
        [data.bucket]
    ).orderBy(
        data.date
    ).rowsBetween(
        -smoothing_lookback, 0
    )

    return data.withColumn(
        "n_", F.mean(data[usage_criteria]).over(windowSpec)
    ).drop(
        usage_criteria
    ).withColumnRenamed(
        "n_", usage_criteria
    )


def jackknifeCountCI(data, string_mode=False):
    count = np.sum(data)
    jackknife_buckets = len(data)
    estimate, bias, stderr, conf_interval = \
        jackknife_stats(np.array(data), np.mean, 0.95)
    if string_mode:
        # TODO: extract a CI formatting function with configurable decimal places
        return "({:.0f} - {:.0f})".format(
            conf_interval[0] * jackknife_buckets, conf_interval[1] * jackknife_buckets
        )
    else:
        return [
            (count - conf_interval[0] * jackknife_buckets),
            (conf_interval[1] * jackknife_buckets - count)
        ]


def jackknifeMeanCI(data, string_mode=False):
    mean = np.mean(data)
    estimate, bias, stderr, conf_interval = \
        jackknife_stats(np.array(data), np.mean, 0.95)
    if string_mode:
        return "({:.0f} - {:.0f})".format(conf_interval[0], conf_interval[1])
    else:
        return [mean - conf_interval[0], conf_interval[1] - mean]


def getPandasDimensionQuery(dimensions):
    if len(dimensions) == 0:
        return ""
    return " and " + " and ".join(["{}=='{}'".format(
        d, dimensions[d]) for d in dimensions]
    )


def dimensionName(dimension):
    return "|".join([dimension[d] for d in dimension])


def longDimensionName(dimension):
    if len(dimension) > 0:
        return "|".join(["{}: {}".format(d, dimension[d]) for d in dimension])
    else:
        return "ALL"
