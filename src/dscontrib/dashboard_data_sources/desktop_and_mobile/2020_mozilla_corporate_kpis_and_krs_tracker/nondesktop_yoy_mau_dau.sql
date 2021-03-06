WITH data as (
SELECT
  submission_date as date,
  'desktop' as product,
  SUM(dau) as dau
FROM
  `moz-fx-data-shared-prod.telemetry.firefox_desktop_exact_mau28_by_dimensions`
WHERE
  submission_date > "2018-12-15"
GROUP BY 1
ORDER BY 1)

SELECT
  *,
  ROUND(AVG(dau) OVER(PARTITION BY product ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) as dau_7d_ma
FROM
  data
ORDER BY 1