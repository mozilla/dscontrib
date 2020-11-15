DECLARE start_load_date DATE DEFAULT '2020-04-26';
DECLARE end_load_date DATE DEFAULT '2020-05-06';

WITH base AS (
    SELECT
      CASE app_name
        WHEN 'Fennec' THEN CONCAT(app_name, ' ', os)
        WHEN 'Focus' THEN CONCAT(app_name, ' ', os)
        WHEN 'Lockbox' THEN CONCAT('Lockwise ', os)
        WHEN 'Zerda' THEN 'Firefox Lite'
      ELSE app_name END AS product_name,
      app_name,
      SPLIT(app_version,'.')[offset(0)] as app_version,
      os,
      normalized_channel,
      country,
      DATE_SUB(submission_date, INTERVAL 6 DAY) AS cohort_date,
      COUNTIF(udf.pos_of_trailing_set_bit(days_created_profile_bits) = 6) AS new_profiles,
      COUNTIF(udf.pos_of_trailing_set_bit(days_created_profile_bits) = 6
      AND BIT_COUNT(days_seen_bits << 1 & udf.bitmask_lowest_7()) > 0) AS day_2_7_activated,
    FROM
      `moz-fx-data-shared-prod.telemetry.nondesktop_clients_last_seen_v1`
    GROUP BY
      product_name, app_name, app_version, submission_date, os, normalized_channel, country )

    SELECT
      *
    FROM
      base
    WHERE
      cohort_date >= start_load_date
      AND cohort_date <= end_load_date
      AND product_name IN ('Fennec Android', 'Focus Android', 'Fennec iOS', 'Focus iOS', 'Firefox Lite',
      'FirefoxConnect', 'Lockwise Android')