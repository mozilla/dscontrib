-- Dashboard Link: https://datastudio.google.com/reporting/1L7dsFyqjT8XZHrYprYS-HCP5_k_gZGIb/page/0iERB

WITH data as (
SELECT
  EXTRACT(YEAR FROM cohort_date) as year,
  cohort_date,
  product,
  SUM(new_profiles) as new_profiles,
  SUM(day_2_7_activated) as activated,
  SAFE_DIVIDE(SUM(day_2_7_activated), SUM(new_profiles)) as day_2_7_activation
FROM
  `moz-fx-data-shared-prod.telemetry.firefox_nondesktop_day_2_7_activation`
GROUP BY
  1,2,3
),

summary as(
SELECT
  *,
  ROUND(AVG(new_profiles) OVER (PARTITION BY product ORDER BY cohort_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS new_profiles_7_day_avg,
  ROUND(AVG(activated) OVER (PARTITION BY product ORDER BY cohort_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS activated_7_day_avg,
  CASE WHEN year = 2019 THEN DATE_ADD(cohort_date, INTERVAL 1 YEAR) ELSE cohort_date END as date_2020
FROM
  data
ORDER BY
  2)

SELECT
  date_2020 as cohort_date,
  product,
  CASE
    WHEN product IN ("Fennec Android", "Focus Android", "Firefox Lite", "Lockwise Android") THEN 'All Android Products'
    WHEN product IN ("Fennec iOS", "Focus iOS") THEN 'All iOS Products'
    ELSE product END as product_group,
  SUM(CASE WHEN year = 2019 THEN new_profiles ELSE 0 END) as new_profiles_2019,
  SUM(CASE WHEN year = 2020 THEN new_profiles ELSE 0 END) as new_profiles_2020,
  SUM(CASE WHEN year = 2019 THEN activated ELSE 0 END) as activated_2019,
  SUM(CASE WHEN year = 2020 THEN activated ELSE 0 END) as activated_2020,
  SUM(CASE WHEN year = 2019 THEN new_profiles_7_day_avg ELSE 0 END) as new_profiles_7_day_avg_2019,
  SUM(CASE WHEN year = 2020 THEN new_profiles_7_day_avg ELSE 0 END) as new_profiles_7_day_avg_2020,
  SUM(CASE WHEN year = 2019 THEN activated_7_day_avg ELSE 0 END) as activated_7_day_avg_2019,
  SUM(CASE WHEN year = 2020 THEN activated_7_day_avg ELSE 0 END) as activated_7_day_avg_2020
FROM
  summary
WHERE
  date_2020 >= "2020-01-01"
	-- To remove the leap year day for yoy comparison
	-- AND date_2020 != "2020-02-29"
  AND date_2020 <= (SELECT max(cohort_date) FROM `moz-fx-data-shared-prod.telemetry.firefox_nondesktop_day_2_7_activation`)
GROUP BY 1,2,3

UNION ALL

SELECT
  date_2020 as cohort_date,
  product,
  'All Products' as product_group,
  SUM(CASE WHEN year = 2019 THEN new_profiles ELSE 0 END) as new_profiles_2019,
  SUM(CASE WHEN year = 2020 THEN new_profiles ELSE 0 END) as new_profiles_2020,
  SUM(CASE WHEN year = 2019 THEN activated ELSE 0 END) as activated_2019,
  SUM(CASE WHEN year = 2020 THEN activated ELSE 0 END) as activated_2020,
  SUM(CASE WHEN year = 2019 THEN new_profiles_7_day_avg ELSE 0 END) as new_profiles_7_day_avg_2019,
  SUM(CASE WHEN year = 2020 THEN new_profiles_7_day_avg ELSE 0 END) as new_profiles_7_day_avg_2020,
  SUM(CASE WHEN year = 2019 THEN activated_7_day_avg ELSE 0 END) as activated_7_day_avg_2019,
  SUM(CASE WHEN year = 2020 THEN activated_7_day_avg ELSE 0 END) as activated_7_day_avg_2020
FROM
  summary
WHERE
  date_2020 >= "2020-01-01"
	-- To remove the leap year day for yoy comparison
	-- AND date_2020 != "2020-02-29"
  AND date_2020 <= (SELECT max(cohort_date) FROM `moz-fx-data-shared-prod.telemetry.firefox_nondesktop_day_2_7_activation`)
GROUP BY 1,2,3