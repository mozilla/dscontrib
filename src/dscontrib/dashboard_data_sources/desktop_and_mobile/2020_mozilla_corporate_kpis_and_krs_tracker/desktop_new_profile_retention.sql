WITH data as (
SELECT
  date,
  'desktop' as product,
  SUM(new_profile_active_in_week_1) as new_profile_active_in_week_1,
  SUM(new_profiles) as new_profiles
FROM
  `moz-fx-data-shared-prod.telemetry.smoot_usage_day_13`
WHERE
  usage = 'Any Firefox Desktop Activity'
  AND attributed = TRUE
  AND date >= "2020-01-01"
  AND date <= CURRENT_DATE-15
GROUP BY
  date)

SELECT
  *,
  ROUND(AVG(new_profile_active_in_week_1) OVER (PARTITION BY product ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) as new_profile_active_in_week_1_7_day_ma,
  ROUND(AVG(new_profiles) OVER (PARTITION BY product ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) as new_profiles_7_day_ma,
  SAFE_DIVIDE(SUM(new_profile_active_in_week_1), SUM(new_profiles)) AS retention
FROM
  data
GROUP BY date,product,new_profile_active_in_week_1, new_profiles
ORDER BY
  date