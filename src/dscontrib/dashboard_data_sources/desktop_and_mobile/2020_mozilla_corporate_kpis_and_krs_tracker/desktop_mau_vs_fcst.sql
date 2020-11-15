with actual as (
SELECT
  submission_date as date,
  "actual" as type,
  SUM(mau) as value
FROM
  `moz-fx-data-shared-prod.telemetry.firefox_desktop_exact_mau28_by_dimensions`
GROUP By 1
),

actual_with_imputed as (
SELECT date, type, value
FROM
(SELECT date, type, value
 FROM actual
 WHERE NOT (`date` BETWEEN '2019-05-15' AND '2019-06-08')
)
UNION ALL
(SELECT date, 'actual' AS type, mau as value
 FROM `moz-fx-data-shared-prod.static.firefox_desktop_imputed_mau28_v1`
 WHERE (`date` BETWEEN '2019-05-15' AND '2019-06-08') AND datasource = "desktop_global"
)
),

forecast as (
SELECT
  date,
  "forecast" as type,
  value as value,
  low90 as low,
  high90 as high
FROM
  `moz-fx-data-shared-prod.telemetry.simpleprophet_forecasts`
WHERE
  datasource = 'Desktop Global MAU'
  AND type = "forecast"
  AND asofdate = (SELECT MAX(asofdate) FROM `moz-fx-data-shared-prod.telemetry.simpleprophet_forecasts`)
)

SELECT
  date,
  value,
  type
FROM
  forecast

UNION ALL

SELECT
  date,
  low,
  'forecast-low' as type
FROM
  forecast

UNION ALL

SELECT
  date,
  high,
  'forecast-high' as type
FROM
  forecast

UNION ALL

SELECT
date,
value,
type
FROM
  actual_with_imputed
order by 1