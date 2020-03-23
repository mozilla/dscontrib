# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import pandas as pd
from fbprophet import Prophet
from dscontrib.jmccrosky.forecast.utils import s2d

# The only holiday we have identified the need to explicitly model is Chinese
# New Year
chinese_new_year = pd.DataFrame({
    'ds': [
        s2d("2016-02-08"), s2d("2017-01-28"), s2d("2018-02-16"),
        s2d("2019-02-05"), s2d("2020-01-25")
    ],
    'holiday': "chinese_new_year",
    'lower_window': -20,
    'upper_window': 20,
})


def forecast(training_data, all_data):
    forecast = {}
    for c in all_data.keys():
        if (len(training_data) < 100):
            continue
        print("Starting with {}".format(c))
        model = Prophet(holidays=chinese_new_year)
        model.fit(training_data[c])
        forecast_period = model.make_future_dataframe(
            periods=(all_data[c].ds.max() - training_data[c].ds.max()).days,
            include_history=False
        )
        if (len(forecast_period) < 100):
            continue
        forecast[c] = model.predict(forecast_period)
        forecast[c]['ds'] = pd.to_datetime(forecast[c]['ds']).dt.date
        forecast[c] = forecast[c][["ds", "yhat", "yhat_lower", "yhat_upper"]]
        forecast[c] = forecast[c].merge(all_data[c], on="ds", how="inner")
        forecast[c]["delta"] = (forecast[c].y - forecast[c].yhat) / forecast[c].y
        forecast[c]["ci_delta"] = 0
        forecast[c].loc[
            forecast[c].delta > 0, "ci_delta"
        ] = forecast[c].delta / (forecast[c].yhat_upper - forecast[c].yhat)
        forecast[c].loc[
            forecast[c].delta < 0, "ci_delta"
        ] = forecast[c].delta / (forecast[c].yhat - forecast[c].yhat_lower)
        forecast[c] = forecast[c][["ds", "geo", "delta", "ci_delta"]]
        print("Done with {}".format(c))
    return forecast
