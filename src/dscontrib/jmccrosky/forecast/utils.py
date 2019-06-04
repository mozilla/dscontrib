# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import numpy as np


# Calculate Mean Absolute Percentage Error of forecast
def calcMape(true, predicted):
    mask = true != 0
    return (np.fabs(true - predicted)/true)[mask].mean() * 100


# Calculate Mean Relative Error of forecast
def calcMre(true, predicted):
    mask = true != 0
    return ((true - predicted)/true)[mask].mean() * 100


# Get most recent date in table
def getLatestDate(bqClient, project, dataset, table, field):
    query = '''
        SELECT
            MAX({field}) as date
        FROM
            `{project}.{dataset}.{table}`
    '''.format(project=project, dataset=dataset, table=table, field=field)
    return bqClient.query(query).to_dataframe()['date'][0]
