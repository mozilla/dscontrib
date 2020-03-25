# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import pandas as pd
from dscontrib.jmccrosky.forecast.utils import s2d
from dscontrib.jmccrosky.anomdtct.data import get_raw_data, prepare_data
from dscontrib.jmccrosky.anomdtct.forecast import forecast


def pipeline(bq_client, bq_storage_client):
    city_raw_data = get_raw_data(
        bq_client,
        bq_storage_client,
        "light_funnel_dau_city"
    )
    (city_clean_data, city_clean_training_data) = prepare_data(
        city_raw_data, s2d('2016-04-08'), s2d('2019-12-31')
    )
    city_forecast_data = forecast(city_clean_training_data, city_clean_data)

    country_raw_data = get_raw_data(
        bq_client,
        bq_storage_client,
        "light_funnel_dau_country"
    )
    (country_clean_data, country_clean_training_data) = prepare_data(
        country_raw_data, s2d('2016-04-08'), s2d('2019-12-31')
    )
    country_forecast_data = forecast(
        country_clean_training_data, country_clean_data
    )
    return pd.concat(
        [city_forecast_data, country_forecast_data],
        ignore_index=True
    )
