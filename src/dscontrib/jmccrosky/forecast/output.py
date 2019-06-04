# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import timedelta

from dscontrib.jmccrosky.forecast.models import setupModels


# Delete output table if necessary and create empty table with appropriate schema
def resetOuputTable(bigquery_client, project, dataset, table_name):
    dataset_ref = bigquery_client.dataset(dataset)
    table_ref = dataset_ref.table(table_name)
    try:
        bigquery_client.delete_table(table_ref)
    except NotFound:
        pass
    schema = [
        bigquery.SchemaField('asofdate', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('datasource', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('type', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('mau', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('low90', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('high90', 'FLOAT', mode='REQUIRED'),
    ]
    table = bigquery.Table(table_ref, schema=schema)
    table = bigquery_client.create_table(table)
    return table


def writeForecasts(bigquery_client, table, model_date, forecast_end, data):
    minYear = np.min([data[k].ds.min() for k in data]).year
    maxYear = forecast_end.year
    years = range(minYear, maxYear + 1)
    models = setupModels(years)
    forecast_start = model_date + timedelta(days=1)
    forecast_period = pd.DataFrame({'ds': pd.date_range(forecast_start, forecast_end)})

    for m in data:
        models[m].fit(data[m].query("ds <= @model_date"))
        forecast = models[m].predict(forecast_period)
        output_data = pd.DataFrame({
            "asofdate": model_date.date(),
            "datasource": m,
            "date": forecast_period.ds.dt.date,
            "type": "forecast",
            "mau": forecast.yhat,
            "low90": forecast.yhat_lower,
            "high90": forecast.yhat_upper,
        })
        errors = bigquery_client.insert_rows(
            table,
            list(output_data.itertuples(index=False, name=None))
        )
        assert errors == []
