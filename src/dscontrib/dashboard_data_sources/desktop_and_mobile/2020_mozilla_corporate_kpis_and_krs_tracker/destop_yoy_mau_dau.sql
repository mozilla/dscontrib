WITH usage_data as(
SELECT
  submission_date as date,
  CASE WHEN EXTRACT(YEAR FROM submission_date) = EXTRACT(YEAR FROM CURRENT_DATE) THEN submission_date ELSE DATE_ADD(submission_date, INTERVAL 1 YEAR) END as current_year_yoy_date,
  "actual" as type,
  SUM(mau) as mau,
  SUM(dau) as dau
FROM
  `moz-fx-data-shared-prod.telemetry.firefox_desktop_exact_mau28_by_dimensions`
WHERE
  submission_date >= "2018-12-15"
GROUP BY 1),

summary_data as (
SELECT
  current_year_yoy_date,
  'desktop' as product,
  SUM(CASE WHEN EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE)  THEN mau ELSE 0 END) as current_year_mau,
  SUM(CASE WHEN EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE)-1 THEN mau ELSE 0 END) as prior_year_mau,
  SUM(CASE WHEN EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE)  THEN dau ELSE 0 END) as current_year_dau,
  SUM(CASE WHEN EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE)-1 THEN dau ELSE 0 END) as prior_year_dau
FROM
  usage_data
GROUP BY 1,2)

SELECT
  *,
  ROUND(AVG(current_year_dau) OVER (PARTITION BY product ORDER BY current_year_yoy_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) as current_year_dau_7d_ma,
  ROUND(AVG(prior_year_dau) OVER (PARTITION BY product ORDER BY current_year_yoy_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) as prior_year_dau_7d_ma
FROM
  summary_data
WHERE
-- remove leap year for better yoy comparisons
  current_year_yoy_date != "2020-02-29"
  -- stop it at current date to keep graph from going to -100% in yoy growth chart
  AND current_year_yoy_date <= (SELECT MAX(submission_date) FROM `moz-fx-data-shared-prod.telemetry.firefox_desktop_exact_mau28_by_dimensions` WHERE submission_date >= CURRENT_DATE -30)
ORDER BY 1