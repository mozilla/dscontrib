# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pandas as pd
import numpy as np
from datetime import timedelta
from collections import defaultdict
from plotly.offline import plot
import plotly.graph_objs as go
from dscontrib.jmccrosky.forecast.utils import s2d, matchDates, getLayout


def _getSinglePrediciton(model, data, trainingEndDate, targetDate):
    model.fit(data.query("ds <= @trainingEndDate"))
    forecast_period = pd.DataFrame({'ds': [s2d(targetDate)]})
    forecast = model.predict(forecast_period)
    return (forecast.yhat[0], forecast.yhat_lower[0], forecast.yhat_upper[0])


def ValidateStability(modelGen, data, trainingEndDateRange, targetDate):
    dates = []
    values = []
    for d in trainingEndDateRange:
        predictions = []
        predictions.append(_getSinglePrediciton(modelGen(), data, d, targetDate))
        values.append(predictions)
        dates.append(d)
    data = pd.DataFrame({
        "date": dates,
        "Predicted MAU for {}".format(targetDate): [i[0][0] for i in values],
        "upper": [i[0][1] for i in values],
        "lower": [i[0][2] for i in values],
    })
    return plot(
        {
            "data": [
                go.Scatter(
                    x=data['date'],
                    y=data["Predicted MAU for {}".format(targetDate)],
                    name="Prediction",
                ),
                go.Scatter(
                    x=data['date'],
                    y=data['upper'],
                    fill='tonexty',
                    mode='none',
                    name='upper 80% CI',
                ),
                go.Scatter(
                    x=data['date'],
                    y=data['lower'],
                    fill='tonexty',
                    mode='none',
                    name='lower 80% CI',
                ),
            ],
            "layout": getLayout(
                ("Predictions of MAU for {} using model "
                 "fit on data up to each date").format(targetDate),
                "Model Trained On Data Up To Date",
                "Prediction for {}".format(targetDate)
            )
        },
        output_type="div"
    )


def _getMetricForRange(model, data, trainingEndDate, metric):
    forecastStart = trainingEndDate + timedelta(days=1)
    forecastEnd = data.ds.max()
    model.fit(data.query("ds <= @trainingEndDate"))
    forecast_period = pd.DataFrame({'ds': pd.date_range(forecastStart, forecastEnd)})
    forecast = model.predict(forecast_period)
    matched = matchDates(
        data.query("ds <= @forecastEnd & ds >= @forecastStart"),
        forecast
    )
    metric = metric(
        np.array(matched.y),
        np.array(matched.yhat),
    )
    return metric


def ValidateMetric(modelGen, data, trainingEndDateRange, metric, metricName):
    dates = []
    mapes = []
    for d in trainingEndDateRange:
        mapes.append(_getMetricForRange(modelGen(), data, d, metric))
        dates.append(d)
    data = pd.DataFrame({"date": dates, metricName: mapes})
    return plot(
        {
            "data": [go.Scatter(x=data['date'], y=data[metricName], name=metricName)],
            "layout": getLayout(
                "{} for model trained up to date".format(metricName),
                "Model Trained On Data Up To Date",
                "{} Metric Value".format(metricName)
            )
        },
        output_type="div"
    )


def _getMetricTrace(model, data, trainingEndDate, metric, metricName):
    forecastStart = trainingEndDate + timedelta(days=1)
    forecastEnd = data.ds.max()
    model.fit(data.query("ds <= @trainingEndDate"))
    forecastPeriod = pd.DataFrame({'ds': pd.date_range(forecastStart, forecastEnd)})
    forecast = model.predict(forecastPeriod)
    matched = matchDates(
        data,
        forecast
    )
    return pd.DataFrame({
        "ds": matched.ds,
        metricName: [
            metric(
                np.array(matched.query("ds == @d").y),
                np.array(matched.query("ds == @d").yhat)
            )
            for d in matched.ds
        ]
    })


def ValidateTraces(modelGen, data, trainingEndDateRange, metric, metricName):
    traces = []
    for d in trainingEndDateRange:
        traces.append(_getMetricTrace(modelGen(), data, d, metric, metricName))
    return plot(
        {
            "data":
                [
                    go.Scatter(x=d['ds'], y=d[metricName])
                    for d in traces
                ],
                "layout": getLayout(
                    "Model Traces of {}".format(metricName),
                    "Prediction Date",
                    "{} Metric Value".format(metricName)
                )
        },
        output_type="div",
    )


def _accumulateHorizonMetrics(model, data, trainingEndDate, metric, metricValues):
    forecastStart = trainingEndDate + timedelta(days=1)
    forecastEnd = data.ds.max()
    model.fit(data.query("ds <= @trainingEndDate"))
    forecastPeriod = pd.DataFrame({'ds': pd.date_range(forecastStart, forecastEnd)})
    forecast = model.predict(forecastPeriod)
    matched = matchDates(
        data,
        forecast
    )
    for d in matched.ds:
        metricValue = metric(
            np.array(matched.query("ds == @d").y),
            np.array(matched.query("ds == @d").yhat)
        )
        horizon = (d - trainingEndDate).days
        metricValues[horizon].append(metricValue)


def ValidateMetricHorizon(modelGen, data, trainingEndDateRange, metric, metricName):
    metricValues = defaultdict(lambda: [])
    for d in trainingEndDateRange:
        _accumulateHorizonMetrics(modelGen(), data, d, metric, metricValues)
    data = pd.DataFrame({
        "horizon": [i for i in metricValues.keys()],
        metricName: [np.mean(metricValues[h]) for h in metricValues.keys()]
    })
    return plot({
        "data": [
            go.Scatter(x=data['horizon'], y=data[metricName], name="metricName"),
        ],
        "layout": getLayout(
            "{} Metric By Model Horizon".format(metricName),
            "Model Horizon",
            "Mean {} Metric Value".format(metricName)
        )
    }, output_type="div")
