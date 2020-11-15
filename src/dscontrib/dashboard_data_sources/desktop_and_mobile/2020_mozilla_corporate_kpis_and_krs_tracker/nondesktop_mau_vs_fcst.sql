with actual as (
SELECT
  submission_date as date,
  "actual" as type,
  SUM(mau) as value
FROM
  `moz-fx-data-shared-prod.telemetry.firefox_nondesktop_exact_mau28_by_dimensions`
WHERE
  product != 'FirefoxForFireTV'
  AND product != 'Firefox Reality'
GROUP By 1,2
ORDER BY 1
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
  datasource = 'Mobile Global MAU'
  AND type = "forecast"
  AND asofdate = (SELECT MAX(asofdate) FROM `moz-fx-data-shared-prod.telemetry.simpleprophet_forecasts`)
ORDER BY 1
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
  actual
order by 1