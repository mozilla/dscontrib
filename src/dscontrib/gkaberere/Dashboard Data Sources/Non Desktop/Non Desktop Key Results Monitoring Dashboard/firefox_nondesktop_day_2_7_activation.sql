WITH data as (
SELECT
  DATE_SUB(submission_date, INTERVAL 6 DAY) AS cohort_date,
  * EXCEPT (submission_date, app_name, os),
  CASE
    WHEN product IN ("Fennec Android", "Focus Android", "Firefox Lite", "Lockwise Android") THEN 'All Android Products'
    WHEN product IN ("Fennec iOS", "Focus iOS") THEN 'All iOS Products'
    ELSE product END as product_group,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.firefox_nondesktop_day_2_7_activation_*`

UNION ALL

SELECT
  DATE_SUB(submission_date, INTERVAL 6 DAY) AS cohort_date,
  * EXCEPT (submission_date, app_name, os),
  'All Products' as product_group,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.firefox_nondesktop_day_2_7_activation_*`
),

averaged as (
SELECT
  *,
  ROUND(AVG(new_profiles) OVER (PARTITION BY product, product_group, country, normalized_channel, app_version ORDER BY cohort_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS new_profiles_7_day_avg,
  ROUND(AVG(day_2_7_activated) OVER (PARTITION BY product, product_group, country, normalized_channel, app_version ORDER BY cohort_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS activated_7_day_avg
FROM
  data)

SELECT
  * EXCEPT (product, product_group),
  CASE WHEN product = "FirefoxConnect" THEN "Echo Show" ELSE product END as product,
  CASE WHEN product_group = "FirefoxConnect" THEN "Echo Show" ELSE product_group END as product_group,
  CASE WHEN EXTRACT(YEAR FROM cohort_date) = EXTRACT(YEAR FROM current_date) THEN new_profiles_7_day_avg ELSE 0 END as new_profiles_7_day_avg_current_year,
  CASE WHEN EXTRACT(YEAR FROM cohort_date) = EXTRACT(YEAR FROM current_date) THEN activated_7_day_avg ELSE 0 END as activated_7_day_avg_current_year,
  CASE WHEN EXTRACT(YEAR FROM cohort_date) = EXTRACT(YEAR FROM current_date)-1 THEN new_profiles_7_day_avg ELSE 0 END as new_profiles_7_day_avg_prior_year,
  CASE WHEN EXTRACT(YEAR FROM cohort_date) = EXTRACT(YEAR FROM current_date)-1 THEN activated_7_day_avg ELSE 0 END as activated_7_day_avg_prior_year
FROM
  averaged