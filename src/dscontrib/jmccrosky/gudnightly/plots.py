# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import numpy as np
import pandas as pd
from pyspark.sql.functions import col
import plotly.graph_objs as go
from plotly.offline import plot

from dscontrib.jmccrosky.gudnightly.usage_criteria import usage_criteria
from dscontrib.jmccrosky.gudnightly.metrics import metricFunctions, metricAggregations
from dscontrib.jmccrosky.gudnightly.metrics import metricDaysNeededPre
from dscontrib.jmccrosky.gudnightly.metrics import metricCIs, metricDaysNeededPost
from dscontrib.jmccrosky.gudnightly.utils import calculateDateWindow, doSmoothing
from dscontrib.jmccrosky.gudnightly.utils import getPandasDimensionQuery, dimensionName
from dscontrib.jmccrosky.gudnightly.utils import jackknifeMeanCI, longDimensionName


def MetricPlot(
    feature_data,
    plot_start_date, plot_end_date,
    criterium, dimensions, metric,
    jackknife_buckets,
    sampling_multiplier,
    smoothing=1,
    extra_filter="TRUE",
    comparison_mode="None",
    transformations=[],
    force_width=None,
    force_height=None,
    x_min=None,
    x_max=None,
    y_min=None,
    y_max=None,
    suppress_ci=False
):
    feature_col = usage_criteria[criterium]
    needed_dimension_variables = list(set().union(*(d.keys() for d in dimensions)))
    buckets_list = ["{:.1f}".format(i) for i in range(jackknife_buckets)]
    date_window = calculateDateWindow(
        plot_start_date, plot_end_date, smoothing, comparison_mode,
        metricDaysNeededPre[metric], metricDaysNeededPost[metric]
    )
    if len(date_window) == 1:
        x = date_window[0]
        intermediate_table = feature_data.filter(feature_data.date.between(x[0], x[1]))
    elif len(date_window) == 2:
        x = date_window[0]
        y = date_window[1]
        intermediate_table = feature_data.filter(
            feature_data.date.between(x[0], x[1]) |
            feature_data.date.between(y[0], y[1])
        )
    else:
        print("need to find a nice way to handle this")
        return
        intermediate_table = intermediate_table.filter(
            extra_filter
        )

    data = metricFunctions[metric](
        intermediate_table, needed_dimension_variables, feature_col, sampling_multiplier
    )

    if smoothing > 1:
        data = doSmoothing(
            data, feature_col,
            " ".join([", {}".format(k) for k in needed_dimension_variables]),
            smoothing - 1
        )

    data = data.filter(
        col('date').between(pd.to_datetime("19000101"), pd.to_datetime("21000101"))
    ).toPandas()
    data['date'] = pd.to_datetime(data.date)

    if "Day Of Week" in transformations:
        dates = [
            "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
        ]
    else:
        dates = pd.date_range(plot_start_date, plot_end_date)
    plot_data = {
        "date": dates,
    }

    if comparison_mode == "Slices":
        comparison_dimensions = dimensions
        dimensions = [{}]
    for dim in dimensions:
        if "Day Of Week" in transformations:
            data['day_of_week'] = data['date'].dt.day_name()
            date_var = "day_of_week"
        else:
            date_var = "date"
        cur_data = data[
            data["date"].isin(pd.date_range(plot_start_date, plot_end_date))
        ]
        if comparison_mode == "Slices":
            cur_all1 = [metricAggregations[metric](
                cur_data.query(
                    "bucket=='ALL' and {}==@d".format(date_var) +
                    getPandasDimensionQuery(comparison_dimensions[0])
                )[feature_col]
            ) for d in dates]
            cur_buckets1 = [[metricAggregations[metric](
                cur_data.query(
                    "bucket==@i and {}==@d".format(date_var) +
                    getPandasDimensionQuery(comparison_dimensions[0])
                )[feature_col]
            ) for i in buckets_list] for d in dates]
            cur_all2 = [metricAggregations[metric](
                cur_data.query(
                    "bucket=='ALL' and {}==@d".format(date_var) +
                    getPandasDimensionQuery(comparison_dimensions[1])
                )[feature_col]) for d in dates]
            cur_buckets2 = [[metricAggregations[metric](
                cur_data.query(
                    "bucket==@i and {}==@d".format(date_var) +
                    getPandasDimensionQuery(comparison_dimensions[1])
                )[feature_col]
            ) for i in buckets_list] for d in dates]
        else:
            cur_all = [metricAggregations[metric](
                cur_data.query(
                    "bucket=='ALL' and {}==@d".format(date_var) +
                    getPandasDimensionQuery(dim)
                )[feature_col]
            ) for d in dates]
            cur_buckets = [[metricAggregations[metric](
                cur_data.query(
                    "bucket==@i and {}==@d".format(date_var) +
                    getPandasDimensionQuery(dim)
                )[feature_col]
            ) for i in buckets_list] for d in dates]

        if comparison_mode in ["Last Year", "YoY"]:
            old_data = data[data["date"].isin(pd.date_range(
                plot_start_date - pd.DateOffset(years=1),
                plot_end_date - pd.DateOffset(years=1)
            ))]
            if "Day Of Week" in transformations:
                def date_offset(x):
                    return x
            else:
                def date_offset(x):
                    return x - pd.DateOffset(years=1)
            old_all = [metricAggregations[metric](old_data.query(
                "bucket=='ALL' and {}==@d".format(date_var) +
                getPandasDimensionQuery(dim))[feature_col]
            ) for d in (date_offset(dates))]
            old_buckets = [[metricAggregations[metric](old_data.query(
                "bucket==@i and {}==@d".format(date_var) +
                getPandasDimensionQuery(dim))[feature_col]
            ) for i in buckets_list] for d in (date_offset(dates))]

        if "Normalize" in transformations:
            total_all = np.sum(cur_all)
            total_buckets = [np.sum(
                [cur_buckets[d][i] for d in range(len(dates))]
            ) for i in range(len(buckets_list))]
            cur_all = [v / float(total_all) for v in cur_all]
            cur_buckets = [[
                    cur_buckets[d][i] / float(total_buckets[i])
                    for i in range(len(buckets_list))
                ] for d in range(len(dates))
            ]
            if comparison_mode != "None":
                total_all = np.sum(old_all)
                total_buckets = [
                    np.sum([old_buckets[d][i] for d in range(len(dates))])
                    for i in range(len(buckets_list))
                ]
                old_all = [v / float(total_all) for v in old_all]
                old_buckets = [[
                    old_buckets[d][i] / float(total_buckets[i])
                    for i in range(len(buckets_list))
                ] for d in range(len(dates))]
        if comparison_mode == "YoY":
            plot_data.update({
                "value" + dimensionName(dim):
                    [(float(c-o)/o) * 100 for c, o in zip(cur_all, old_all)],
                "ci" + dimensionName(dim):
                    [
                        jackknifeMeanCI([(float(i-j)/j) * 100 for i, j in zip(c, o)])
                        for c, o in zip(cur_buckets, old_buckets)
                    ],
            })
        if comparison_mode == "Slices":
            plot_data.update({
                "value" + dimensionName(dim):
                    [(float(c1)/c2) * 100 for c1, c2 in zip(cur_all1, cur_all2)],
                "ci" + dimensionName(dim):
                    [jackknifeMeanCI(
                        [(float(i)/j) * 100 for i, j in zip(c1, c2)]
                    ) for c1, c2 in zip(cur_buckets1, cur_buckets2)],
            })
        if comparison_mode == "Last Year":
            plot_data.update({
                "value" + dimensionName(dim): cur_all,
                "ci" + dimensionName(dim): [metricCIs[metric](i) for i in cur_buckets],
                "value" + dimensionName(dim) + " last year": old_all,
                "ci" + dimensionName(dim) + " last year":
                    [metricCIs[metric](i) for i in old_buckets],
            })
        if comparison_mode == "None":
            plot_data.update({
                "value" + dimensionName(dim): cur_all,
                "ci" + dimensionName(dim):
                    [metricCIs[metric](i) for i in cur_buckets],
            })

    plot_data = pd.DataFrame(plot_data)
    plotly_data = [
        go.Scatter(
            name=(
                longDimensionName(dim) if comparison_mode != "Slices"
                else "{} as % of {}".format(
                    longDimensionName(comparison_dimensions[0]),
                    longDimensionName(comparison_dimensions[1])
                )
            ),
            x=plot_data.date,
            y=plot_data["value" + dimensionName(dim)],
            error_y=(dict(
                type='data',
                symmetric=False,
                array=[ci[0] for ci in plot_data["ci" + dimensionName(dim)]],
                arrayminus=[c[1] for c in plot_data["ci" + dimensionName(dim)]],
            ) if not suppress_ci else None)
        ) for dim in dimensions] + ([
            go.Scatter(
                name=longDimensionName(dim) + " last year",
                x=plot_data.date,
                y=plot_data["value" + dimensionName(dim) + " last year"],
                error_y=(dict(
                    type='data',
                    symmetric=False,
                    array=[
                        ci[0] for ci in
                        plot_data["ci" + dimensionName(dim) + " last year"]
                    ],
                    arrayminus=[
                        ci[1] for ci in
                        plot_data["ci" + dimensionName(dim) + " last year"]
                    ],
                ) if not suppress_ci else None)
            )
            for dim in dimensions] if comparison_mode == "Last Year" else []
    )
    x_label = "Date"
    ytitle = '{}{}'.format(
        metric, (
            " (Proportion)" if "Normalize" in transformations
            else (" (YoY Percent Change)" if comparison_mode == "YoY" else "")
        )
    )
    if comparison_mode == "Slices":
        ytitle = "Percent of {}".format(metric)
    if (metric in [
        "Week 1 Retention (excluding single-day profiles)",
        "Week 1 Retention"
    ]) or (criterium == "New Profile"):
        x_label = "Profile Creation Date"
    layout = go.Layout(
        showlegend=(True if comparison_mode == "Slices" else None),
        autosize=force_width is None,
        width=force_width,
        height=force_height,
        title=(
            '{} ({}) Over Time'.format(metric, criterium) + (
                " (Restricted to {})".format(dimensionName(dimensions[0]))
                if (len(dimensions) == 1 and len(dimensions[0]) != 0) else ""
            )
        ),
        xaxis=dict(
            title=x_label,
            titlefont=dict(
                family='Courier New, monospace',
                size=18,
                color='#7f7f7f'
            ),
            range=([x_min, x_max] if x_min is not None else None)
        ),
        yaxis=dict(
            title=ytitle,
            titlefont=dict(
                family='Courier New, monospace',
                size=18,
                color='#7f7f7f'
            ),
            range=([y_min, y_max] if y_min is not None else None)
        )
    )
    return plot({"data": plotly_data, "layout": layout}, output_type='div')
