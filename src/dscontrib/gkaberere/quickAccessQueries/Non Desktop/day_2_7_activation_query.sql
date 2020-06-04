DECLARE cohort_start_date DATE DEFAULT "2020-05-07";
DECLARE cohort_end_date DATE DEFAULT "2020-05-13";
DECLARE cohort_start_date_prior_year DATE DEFAULT "2019-05-07";
DECLARE cohort_end_date_prior_year DATE DEFAULT "2019-05-13";

WITH data as (
SELECT
  DATE_SUB(submission_date, INTERVAL 6 DAY) AS cohort_date,
  * EXCEPT (submission_date, app_name, os)
FROM
  `moz-fx-data-shared-prod.telemetry_derived.firefox_nondesktop_day_2_7_activation_*`
),

unioned as (
SELECT
  cohort_date,
  product,
  app_version,
  normalized_channel,
  country,
  new_profiles,
  day_2_7_activated
FROM
  data

UNION ALL

SELECT
  cohort_date,
  'All Products' as product,
  app_version,
  normalized_channel,
  country,
  SUM(new_profiles) as new_profiles,
  SUM(day_2_7_activated) as day_2_7_activated
FROM
  data
GROUP BY 1,2,3,4,5

UNION ALL

SELECT
  cohort_date,
  CASE
    WHEN product IN ("Fennec Android", "Focus Android", "Firefox Lite", "Lockwise Android") THEN 'All Android Products'
    WHEN product IN ("Fennec iOS", "Focus iOS") THEN 'All iOS Products' END as product,
  app_version,
  normalized_channel,
  country,
  SUM(new_profiles) as new_profiles,
  SUM(day_2_7_activated) as day_2_7_activated
FROM
  data
WHERE
  product != 'FirefoxConnect'
GROUP BY 1,2,3,4,5
),

averaged as (

SELECT
  *,
  ROUND(AVG(new_profiles) OVER (PARTITION BY product, country, normalized_channel, app_version ORDER BY cohort_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS new_profiles_7_day_avg,
  ROUND(AVG(day_2_7_activated) OVER (PARTITION BY product, country, normalized_channel, app_version ORDER BY cohort_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS activated_7_day_avg
FROM
  unioned)

SELECT
  cohort_date,
  product,
  SUM(new_profiles) as new_profiles,
  SUM(day_2_7_activated) as day_2_7_activated,
  SUM(new_profiles_7_day_avg) as new_profiles_7_day_avg,
  SUM(activated_7_day_avg) as activated_7_day_avg
FROM
  averaged
WHERE
  (cohort_date >= cohort_start_date
  AND cohort_date <= cohort_end_date)
  OR
  (cohort_date >= cohort_start_date_prior_year
  AND cohort_date <= cohort_end_date_prior_year)
GROUP BY 1,2
ORDER BY 2,1