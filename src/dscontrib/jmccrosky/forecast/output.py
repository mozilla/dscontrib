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
    ] + [
        bigquery.SchemaField('p{}'.format(q), 'FLOAT', mode='REQUIRED')
        for q in range(10, 100, 10)
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
    forecast_period['ds']

    for m in data:
        models[m].fit(data[m].query("ds <= @model_date"))
        forecastSamples = models[m].sample_posterior_predictive(
            models[m].setup_dataframe(forecast_period)
        )
        output_data = {
            "asofdate": model_date,
            "datasource": m,
            "date": forecast_period['ds'].dt.date,
            "type": "forecast",
            "mau": np.mean(forecastSamples['yhat'], axis=1),
            "low90": np.nanpercentile(forecastSamples['yhat'], 5, axis=1),
            "high90": np.nanpercentile(forecastSamples['yhat'], 95, axis=1),
        }
        output_data.update({
            "p{}".format(q): np.nanpercentile(forecastSamples['yhat'], q, axis=1)
            for q in range(10, 100, 10)
        })
        errors = bigquery_client.insert_rows(
            table,
            list(pd.DataFrame(output_data).itertuples(index=False, name=None))
        )
        assert errors == []
