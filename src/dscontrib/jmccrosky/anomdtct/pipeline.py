# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from dscontrib.jmccrosky.forecast.utils import s2d
from dscontrib.jmccrosky.anomdtct.data import get_raw_data, prepare_data
from dscontrib.jmccrosky.anomdtct.forecast import forecast


def pipeline(bq_client, bq_storage_client):
    raw_data = get_raw_data(
        bq_client,
        bq_storage_client,
        "light_funnel_sampled_mau_city"
    )
    (clean_data, clean_training_data) = prepare_data(
        raw_data, s2d('2016-04-08'), s2d('2019-12-31')
    )
    forecast_data = forecast(clean_training_data, clean_data)
    return forecast_data
