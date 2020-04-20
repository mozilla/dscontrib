DECLARE cohort_date DATE DEFAULT '2020-03-01';
DECLARE activation_period_start DATE DEFAULT '2020-03-02';
DECLARE activation_period_end DATE DEFAULT '2020-03-07';
/*DECLARE os_name STRING DEFAULT 'Android';
DECLARE app STRING DEFAULT 'Fennec';*/

WITH last_seen_data as (
SELECT
  submission_date,
  client_id,
  days_since_seen,
  days_since_created_profile,
  profile_date,
  app_name,
  SPLIT(metadata_app_version,'.')[offset(0)] as app_version,
  os,
  normalized_channel,
  campaign,
  country,
  distribution_id,
  CASE app_name
    WHEN 'Fennec' THEN CONCAT(app_name, ' ', os)
    WHEN 'Focus' THEN CONCAT(app_name, ' ', os)
    WHEN 'Lockbox' THEN CONCAT('Lockwise ', os)
    WHEN 'Zerda' THEN 'Firefox Lite'
    ELSE app_name
  END AS product_name
FROM
  `moz-fx-data-shared-prod.telemetry.core_clients_last_seen`
WHERE
  submission_date >= cohort_date
  AND submission_date <= activation_period_end
  /*AND app_name = app
  AND os = os_name*/
  AND days_since_seen < 1),

cohort as (
SELECT
  *
FROM
  last_seen_data
WHERE
-- filtering specifically for where the two dates match as the profile creation date sees more profiles created in days after the submission date i.e. Mar 6 has more client_ids with Mar 1 profile creation date than Mar 1
-- Shouldn't be a material difference in the activation but should revalidate when productionizing
  profile_date = cohort_date
  AND submission_date = cohort_date
  AND product_name IN ('Fennec Android', 'Fenix', 'Focus Android', 'Fennec iOS', 'Focus iOS', 'Firefox Lite', 'FirefoxConnect', 'Lockwise Android')
  ),

usage as (
SELECT
  client_id,
  COUNT(DISTINCT submission_date) as days_active
FROM
  last_seen_data
WHERE
  submission_date >= activation_period_start
  AND submission_date <= activation_period_end
GROUP BY 1),

activation as (
SELECT
  cohort.*,
  usage.days_active
FROM
  cohort
LEFT JOIN
  usage
ON
  cohort.client_id = usage.client_id)

SELECT
  submission_date,
  product_name,
  COUNT(client_id) as new_profiles,
  SUM(IF(days_active IS NOT NULL, 1, 0)) as day_2_7_activated
FROM
  activation
GROUP BY 1,2