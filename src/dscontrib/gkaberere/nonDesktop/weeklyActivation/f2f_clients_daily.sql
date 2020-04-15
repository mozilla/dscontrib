DECLARE lookback INT64 DEFAULT 6;

--Determine the start dates and end dates of the ETL using the data and today's date
DECLARE start_date_etl DATE DEFAULT
(
SELECT
  MAX(start_date) AS start_date_etl
FROM
  `moz-fx-data-bq-data-science.mgorlick.f2f__clients_daily` a
);

DECLARE end_date_etl DATE DEFAULT CURRENT_DATE();

--Delete incomplete data
DELETE `moz-fx-data-bq-data-science.mgorlick.f2f__clients_daily`
WHERE start_date BETWEEN DATE_SUB(start_date_etl, INTERVAL lookback DAY) AND start_date_etl;


--Insert statement
INSERT `moz-fx-data-bq-data-science.mgorlick.f2f__clients_daily`

WITH

--Fennec Android Clean
--bringing in all normalized_channels so I can monitor the rollout.
  FennecBase AS (
  SELECT
    metadata.uri.app_name AS app_name,
    'core' AS ping_type,
    sample_id,
    document_id,
    os,
    osversion AS os_version,
    locale,
    metadata.geo.country AS country,
    metadata.geo.city AS city,
    CAST(NULL AS STRING) AS device_manufacturer,
    device AS device_model,
    metadata.uri.app_build_id AS app_build,
    normalized_channel AS normalized_channel,
    CAST(arch AS STRING) AS architecture,
    display_version AS app_display_version,
    SAFE.DATE_FROM_UNIX_DATE(profile_date) profile_created_date,
    SAFE.DATE(SAFE.TIMESTAMP(created)) AS start_date,
    SAFE.TIMESTAMP(NULL) AS start_timestamp,
    SAFE.DATE(SAFE.TIMESTAMP(created)) AS end_date,
    SAFE.DATE(submission_timestamp) AS submission_date,
    submission_timestamp,
    LOWER(client_id) AS client_id,
    campaign_id,
    CAST(CASE
      WHEN normalized_os_version !='' THEN SAFE_CAST(normalized_os_version as INT64) >= 21
      ELSE FALSE END AS STRING) AS os_version_compatible,
    CAST(normalized_os_version as STRING)  AS os_api_level,
    CAST(NULL as STRING) AS normalized_os_version,
    metadata.uri.app_version AS app_version,
    CAST(SPLIT(metadata.uri.app_version, '.')[OFFSET(0)] AS STRING) AS app_version_major,
    durations,
    CASE
      WHEN default_search LIKE '%google%' THEN 'Google'
      WHEN default_search LIKE '%ddg%' THEN 'DuckDuckGo'
      WHEN default_search LIKE '%bing%' THEN 'Bing'
      WHEN default_search LIKE '%yandex%' THEN 'Yandex'
      WHEN default_search LIKE '%baidu%' THEN 'Baidu'
      WHEN default_search LIKE '%qwant%' THEN 'Qwant'
      WHEN default_search LIKE '%twitter%' THEN 'Twitter'
      WHEN default_search LIKE '%wiki%' THEN 'Wikipedia'
      WHEN default_search LIKE '%yahoo%' THEN 'Yahoo'
      WHEN default_search LIKE '%amazon%' THEN 'Amazon'

      ELSE NULL END AS search_default_engine_name,
    default_search AS search_default_engine_code,
    default_browser AS default_browser
  FROM
    `moz-fx-data-shared-prod.telemetry.core`
  WHERE
    client_id IS NOT NULL
    AND normalized_os = 'Android' AND normalized_app_name = 'Fennec' AND normalized_channel IN ('release', 'nightly', 'beta')
    --AND DATE(submission_timestamp) BETWEEN start_date_etl AND DATE_ADD(end_date_etl, INTERVAL lookback DAY)
    AND DATE(submission_timestamp) BETWEEN DATE_SUB(start_date_etl, INTERVAL lookback DAY) AND DATE_ADD(end_date_etl, INTERVAL lookback DAY)
    ),

--Fenix Android Unioned
--Missing First Install ping; https://github.com/mozilla-mobile/fenix/issues/7295
  FenixUnioned AS (
   SELECT
    'baseline' AS ping_type,
    submission_timestamp,
    ping_info.start_time,
    ping_info.end_time,
    document_id,
    client_info,
    sample_id,
    metadata,
    'preview beta' AS normalized_channel,
    metrics.string.glean_baseline_locale AS locale,
    metrics.timespan.glean_baseline_duration.value AS durations,
    NULL AS metrics_metrics,
    CAST(NULL AS STRING) AS campaign_id, -- does not exist on this ping, metrics ping. metrics.string.metrics_adjust_campaign
    normalized_os_version
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.baseline_v1`
  WHERE DATE(submission_timestamp) BETWEEN start_date_etl AND DATE_ADD(end_date_etl, INTERVAL lookback DAY)
  UNION ALL

SELECT
    'baseline' AS ping_type,
    submission_timestamp,
    ping_info.start_time,
    ping_info.end_time,
    document_id,
    client_info,
    sample_id,
    metadata,
    'preview nightly' AS normalized_channel,
    metrics.string.glean_baseline_locale AS locale,
    metrics.timespan.glean_baseline_duration.value AS durations,
    NULL AS metrics_metrics,
    CAST(NULL AS STRING) AS campaign_id, -- does not exist on this ping, metrics ping. metrics.string.metrics_adjust_campaign
    normalized_os_version
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.baseline_v1`
  WHERE DATE(submission_timestamp) BETWEEN start_date_etl AND DATE_ADD(end_date_etl, INTERVAL lookback DAY)
  UNION ALL
 --post migration nightly
 SELECT
    'baseline' AS ping_type,
    submission_timestamp,
    ping_info.start_time,
    ping_info.end_time,
    document_id,
    client_info,
    sample_id,
    metadata,
    'nightly' AS normalized_channel,
    metrics.string.glean_baseline_locale AS locale,
    metrics.timespan.glean_baseline_duration.value AS durations,
    NULL AS metrics_metrics,
    CAST(NULL AS STRING) AS campaign_id, -- does not exist on this ping, metrics ping. metrics.string.metrics_adjust_campaign
    normalized_os_version
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.baseline_v1`
  WHERE DATE(submission_timestamp) BETWEEN start_date_etl AND DATE_ADD(end_date_etl, INTERVAL lookback DAY)

   --post migration beta
  UNION ALL
  SELECT
    'baseline' AS ping_type,
    submission_timestamp,
    ping_info.start_time,
    ping_info.end_time,
    document_id,
    client_info,
    sample_id,
    metadata,
    'beta' AS normalized_channel,
    metrics.string.glean_baseline_locale AS locale,
    metrics.timespan.glean_baseline_duration.value AS durations,
    NULL AS metrics_metrics,
    CAST(NULL AS STRING) AS campaign_id, -- does not exist on this ping, metrics ping. metrics.string.metrics_adjust_campaign
    normalized_os_version
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.baseline_v1`
  WHERE DATE(submission_timestamp) BETWEEN start_date_etl AND DATE_ADD(end_date_etl, INTERVAL lookback DAY)

  --post migration release
  UNION ALL
  SELECT
    'baseline' AS ping_type,
    submission_timestamp,
    ping_info.start_time,
    ping_info.end_time,
    document_id,
    client_info,
    sample_id,
    metadata,
    'release' AS normalized_channel,
    metrics.string.glean_baseline_locale AS locale,
    metrics.timespan.glean_baseline_duration.value AS durations,
    NULL AS metrics_metrics,
    CAST(NULL AS STRING) AS campaign_id, -- does not exist on this ping, metrics ping. metrics.string.metrics_adjust_campaign
    normalized_os_version
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.baseline_v1`
  WHERE DATE(submission_timestamp) BETWEEN start_date_etl AND DATE_ADD(end_date_etl, INTERVAL lookback DAY)
  ),


--Fenix Android Clean
  FenixBase AS (
  SELECT
    'Fenix' AS app_name,
    ping_type,
    sample_id,
    document_id,
    client_info.os AS os,
    client_info.os_version AS os_version,
    locale,
    metadata.geo.country AS country,
    metadata.geo.city AS city,
    client_info.device_manufacturer AS device_manufacturer,
    client_info.device_model AS device_model,
    client_info.app_build AS app_build,
    normalized_channel,
    client_info.architecture AS architecture,
    client_info.app_display_version AS app_display_version,
    SAFE.PARSE_DATE('%F', SUBSTR(client_info.first_run_date, 1, 10)) AS profile_created_date, -- This is the date in the timezone where the telemetry was recorded.  While we know what timezone it was we do not know the time to bring the date to UTC.
    SAFE.DATE(SAFE.TIMESTAMP(CONCAT(SUBSTR(start_time,1,16),':00', SUBSTR(start_time,17,20)))) start_date, --UTC
    SAFE.TIMESTAMP(CONCAT(SUBSTR(start_time,1,16),':00', SUBSTR(start_time,17,20))) start_timestamp,  --UTC
    SAFE.DATE(SAFE.TIMESTAMP(CONCAT(SUBSTR(end_time,1,16),':00', SUBSTR(start_time,17,20)))) end_date,  --UTC
    SAFE.DATE(submission_timestamp) AS submission_date, --UTC
    submission_timestamp,
    LOWER(client_info.client_id) AS client_id,
    campaign_id,
    CAST(CASE
      WHEN normalized_os_version !='' THEN SAFE_CAST(SPLIT(normalized_os_version,'.')[OFFSET(0)] AS INT64) >= 5
      ELSE FALSE END AS STRING) AS os_version_compatible,  -- this is normalized_os_version.  Fennec uses the API level. https://en.wikipedia.org/wiki/Android_version_history
    CAST(NULL AS STRING)  AS os_api_level,
    CAST(SPLIT(normalized_os_version,'.')[OFFSET(0)] AS STRING) AS normalized_os_version,
    client_info.app_display_version AS app_version,
    CAST(SPLIT(client_info.app_display_version, '.')[OFFSET(0)] AS STRING) AS app_version_major,
    durations,
    CAST(NULL AS STRING) AS search_default_engine_name,
    CAST(NULL AS STRING) AS search_default_engine_code,
    CAST(NULL AS BOOL) AS default_browser
  FROM
    FenixUnioned
  WHERE
    client_info.client_id IS NOT NULL
    ),

--Union Android Browsers
  ProductUnion AS
    (
    SELECT
      *
     FROM
     FennecBase
    UNION ALL
    SELECT
      *
     FROM
     FenixBase
    ),

--Calculate time differences for QA and Insights
  DataDiff AS (
  SELECT
    DATE_DIFF(start_date, profile_created_date, DAY) AS date_diff_first_start,
    TIMESTAMP_DIFF(submission_timestamp, start_timestamp, HOUR) AS hour_diff_start_submit,
    DATE_DIFF(submission_date, start_date,  DAY) AS date_diff_start_submit,
    *
  FROM
    ProductUnion
  WHERE
    DATE_DIFF(submission_date, start_date,  DAY) BETWEEN 0 AND lookback
    AND start_date >= start_date_etl
    AND client_id NOT IN ('c0ffeec0-ffee-c0ff-eec0-ffeec0ffeec0')
),


windowed AS (
  SELECT
    start_date,
    client_id,
    app_name,
    normalized_channel,
    ROW_NUMBER() OVER w1_unframed AS _n,
    SUM(IF(durations BETWEEN 0 AND 100000, durations, 0)) OVER w1  AS durations,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(profile_created_date) OVER w1) AS profile_created_date,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(submission_date) OVER w1) AS submission_date,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(end_date) OVER w1) AS end_date,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(date_diff_first_start) OVER w1) AS date_diff_first_start,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(hour_diff_start_submit) OVER w1) AS hour_diff_start_submit,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(date_diff_start_submit) OVER w1) AS date_diff_start_submit,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(campaign_id) OVER w1) AS campaign_id,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(locale) OVER w1) AS locale,
    `moz-fx-data-shared-prod.udf.json_mode_last`(ARRAY_AGG(`moz-fx-data-shared-prod.udf.geo_struct`(country, city, NULL, NULL)) OVER w1).* EXCEPT (geo_subdivision1, geo_subdivision2),
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(device_manufacturer) OVER w1) AS device_manufacturer,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(device_model) OVER w1) AS device_model,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(app_build) OVER w1) AS app_build,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(architecture) OVER w1) AS architecture,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(normalized_os_version) OVER w1) AS normalized_os_version,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(os_api_level) OVER w1) AS os_api_level,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(os_version_compatible) OVER w1) AS os_version_compatible_fenix,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(app_version_major) OVER w1) AS app_version_major,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(search_default_engine_name) OVER w1) AS search_default_engine_name,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(search_default_engine_code) OVER w1) AS search_default_engine_code,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(default_browser) OVER w1) AS default_browser,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(app_display_version) OVER w1) AS app_display_version
  FROM
    DataDiff
  WHERE start_date BETWEEN start_date_etl AND end_date_etl
  WINDOW
    w1 AS (
    PARTITION BY
      client_id,
      start_date
    ORDER BY
      start_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
    -- We must provide a modified window for ROW_NUMBER which cannot accept a frame clause.
    w1_unframed AS (
    PARTITION BY
      client_id,
      start_date
    ORDER BY
      start_timestamp) )
SELECT
  * EXCEPT (_n)
FROM
  windowed
WHERE
  _n = 1
"""