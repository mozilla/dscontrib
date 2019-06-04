# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pandas as pd


_queries = {
    "desktop": '''
        SELECT
            submission_date as date,
            sum(mau) AS global_mau,
            SUM(IF(country IN ('US', 'FR', 'DE', 'GB', 'CA'), mau, 0)) AS tier1_mau
        FROM
            `moz-fx-data-derived-datasets.telemetry.firefox_desktop_exact_mau28_by_dimensions_v1`
        GROUP BY
            date
        ORDER BY
            date
    ''',
    "nondesktop": '''
        SELECT
            submission_date as date,
            sum(mau) AS global_mau,
            SUM(IF(country IN ('US', 'FR', 'DE', 'GB', 'CA'), mau, 0)) AS tier1_mau
        FROM
            `moz-fx-data-derived-datasets.telemetry.firefox_nondesktop_exact_mau28_by_dimensions_v1`
        GROUP BY
            date
        ORDER BY
            date
    ''',
    "fxa": '''
        SELECT
            submission_date AS date,
            SUM(mau) AS global_mau,
            SUM(seen_in_tier1_country_mau) AS tier1_mau
        FROM
            `moz-fx-data-derived-datasets.telemetry.firefox_accounts_exact_mau28_by_dimensions_v1`
        GROUP BY
            submission_date
        ORDER BY
            submission_date
    '''
}


def getKpiData(bqClient):
    data = {}
    for q in _queries:
        rawData = bqClient.query(_queries[q]).to_dataframe()
        data['{}_global'.format(q)] = rawData[
            ["date", "global_mau"]
        ].rename(
            index=str, columns={"date": "ds", "global_mau": "y"}
        )
        data['{}_tier1'.format(q)] = rawData[
            ["date", "tier1_mau"]
        ].rename(
            index=str, columns={"date": "ds", "tier1_mau": "y"}
        )
    for k in data:
        data[k]['ds'] = pd.to_datetime(data[k]['ds']).dt.date
    return data
