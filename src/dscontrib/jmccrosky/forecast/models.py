# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from fbprophet import Prophet
import pandas as pd
from datetime import date, timedelta


# Get easter dates
def getEasters(year):
    a = year % 19
    b = year // 100
    c = year % 100
    d = (19 * a + b - b // 4 - ((b - (b + 8) // 25 + 1) // 3) + 15) % 30
    e = (32 + 2 * (b % 4) + 2 * (c // 4) - d - (c % 4)) % 7
    f = d + e - 7 * ((a + 11 * d + 22 * e) // 451) + 114
    month = f // 31
    day = f % 31 + 1
    easter = date(year, month, day)
    return [("easter{}".format(x), easter + timedelta(days=x)) for x in range(-3, 2)]


# Get holidays dataframe in prophet's format
def getHolidays(years):
    easters = pd.DataFrame({
        'ds': [e[1] for i in years for e in getEasters(i)],
        'holiday': [e[0] for i in years for e in getEasters(i)],
        'lower_window': 0,
        'upper_window': 0,
    })
    return easters


def setupModels(years):
    models = {}
    models["desktop_global"] = Prophet(
        yearly_seasonality=20,
        changepoint_range=0.7,
        seasonality_mode='multiplicative',
        changepoint_prior_scale=0.008,
        seasonality_prior_scale=0.20,
        holidays=getHolidays(years)
    )
    models["nondesktop_global"] = Prophet()
    models["fxa_global"] = Prophet()
    models["desktop_tier1"] = Prophet()
    models["nondesktop_tier1"] = Prophet()
    models["fxa_tier1"] = Prophet()
    models["Fennec Android"] = Prophet(
        changepoint_prior_scale=0.0005,
        seasonality_prior_scale=0.001,
        seasonality_mode='multiplicative'
    )
    models["Focus iOS"] = Prophet(changepoint_prior_scale=0.0005)
    models["Focus Android"] = Prophet(changepoint_prior_scale=0.005)
    models["Fennec iOS"] = Prophet(
        hangepoint_prior_scale=0.005,
        seasonality_prior_scale=0.001,
        seasonality_mode='multiplicative'
    )
    models["Fenix"] = Prophet(changepoint_prior_scale=0.0005)
    models["Firefox Lite"] = Prophet(changepoint_prior_scale=0.0005)
    models["FirefoxForFireTV"] = Prophet(
        changepoint_prior_scale=0.0005,
        seasonality_prior_scale=0.005,
        seasonality_mode='multiplicative',
        yearly_seasonality=True
    )
    models["FirefoxConnect"] = Prophet(changepoint_prior_scale=0.0005)
    return models


def setupDataFilters():
    filters = {}
    filters["desktop_global"] = lambda x: x
    filters["nondesktop_global"] = lambda x: x
    filters["fxa_global"] = lambda x: x
    filters["desktop_tier1"] = lambda x: x
    filters["nondesktop_tier1"] = lambda x: x
    filters["fxa_tier1"] = lambda x: x
    filters["Fennec Android"] = lambda x: x.query("ds >= '2017-03-04'")
    filters["Focus iOS"] = lambda x: x.query("ds >= '2017-12-06'")
    filters["Focus Android"] = lambda x: x.query(
        "(ds >= '2017-07-17') & ((ds < '2018-09-01') | (ds > '2019-03-01'))"
    )
    filters["Fennec iOS"] = lambda x: x.query(
        "(ds >= '2017-03-03') & ((ds < '2017-11-08') | (ds > '2017-12-31'))"
    )
    filters["Fenix"] = lambda x: x.query("ds >= '2019-07-03'")
    filters["Firefox Lite"] = lambda x: x.query("ds >= '2019-05-17'")
    filters["FirefoxForFireTV"] = lambda x: x.query("ds >= '2018-02-04'")
    filters["FirefoxConnect"] = lambda x: x.query("ds >= '2018-10-10'")
    return filters
