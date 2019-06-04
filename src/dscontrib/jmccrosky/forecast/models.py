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
def GetHolidays(years):
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
        holidays=GetHolidays(years)
    )
    models["nondesktop_global"] = Prophet()
    models["fxa_global"] = Prophet()
    models["desktop_tier1"] = Prophet()
    models["nondesktop_tier1"] = Prophet()
    models["fxa_tier1"] = Prophet()
    return models
