-- https://datastudio.google.com/reporting/1L7dsFyqjT8XZHrYprYS-HCP5_k_gZGIb/page/fujVB/edit

-- Query to power yoy tracking of unique pageviews for the app stores, a 2020 KR
WITH playstore_data as (
-- Pull all playstore data by source
SELECT
  EXTRACT(YEAR FROM Date) as Year,
  Date,
  Package_Name,
  Acquisition_Channel,
  store_listing_visitors,
  Installers,
  'Android' as os,
  -- 7 day averaging for easier to read visualizations
  ROUND(AVG(store_listing_visitors) OVER (PARTITION BY Package_Name, Acquisition_Channel ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS store_listing_visitors_7_day_avg,
  ROUND(AVG(installers) OVER (PARTITION BY Package_Name, Acquisition_Channel ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS installers_7_day_avg
FROM
  `moz-fx-data-marketing-prod.google_play_store.p_Retained_installers_channel_v1`
WHERE
  Date >= "2019-01-01"),

apple_data as (
-- Pull all apple store data by source
SELECT
  * EXCEPT(app_name, active_devices_opt_in, active_devices_last_30_days_opt_in, deletions_opt_in, installations_opt_in, sessions_opt_in),
  CASE
    WHEN app_name = 'Focus' THEN 'Focus iOS'
    WHEN app_name = 'Firefox' THEN 'Fennec iOS'
    WHEN app_name = 'Lockwise' THEN 'Firefox Lockwise iOS'
    WHEN app_name = 'Klar' THEN 'Firefox Klar iOS'
    ELSE CONCAT(app_name, ' iOS') END as browser,
  'iOS' as os,
  -- 7 day averaging for easier to read visualizations
  ROUND(AVG(impressions) OVER (PARTITION BY app_name, source ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS impressions_7_day_avg,
  ROUND(AVG(impressions_unique_device) OVER (PARTITION BY app_name, source ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS impressions_unique_device_7_day_avg,
  ROUND(AVG(product_page_views) OVER (PARTITION BY app_name, source ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS pageviews_7_day_avg,
  ROUND(AVG(product_page_views_unique_device) OVER (PARTITION BY app_name, source ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS pageviews_unique_device_7_day_avg,
  ROUND(AVG(app_units) OVER (PARTITION BY app_name, source ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS app_units_7_day_avg
FROM
  `moz-fx-data-marketing-prod.apple_app_store.metrics_by_source`
WHERE
  Date >= "2019-01-01"),

max_date as (
-- Gplay vs Apple Store data comes in at different cadences. Limiting to the minimum max(date) between the two data sources to avoid confusion
SELECT
  MIN(end_date) as end_date
FROM(
  SELECT
    MAX(date) as end_date
  FROM
    `moz-fx-data-marketing-prod.apple_app_store.metrics_by_source`
  UNION ALL
  SELECT
    max(date) as end_date
  FROM
    `moz-fx-data-marketing-prod.google_play_store.p_Retained_installers_channel_v1`))

-- Android products by product
SELECT
  date,
  Package_Name,
  CASE
    -- Fennec Android
    -- 2020-06-08 was when Beta channel was fully migrated to Fenix
    WHEN Package_Name =	'org.mozilla.firefox' THEN 'Fennec Android'
    WHEN date < "2020-06-08" AND Package_Name =	'org.mozilla.firefox_beta' THEN 'Fennec Android Beta'
    WHEN Package_Name =	'org.mozilla.fennec_aurora' THEN 'Fennec Android Nightly'
    -- Fenix
    WHEN date < "2020-06-08" AND Package_Name =	'org.mozilla.fenix' THEN 'Fenix'
    WHEN date >= "2020-06-08" AND Package_Name =	'org.mozilla.fenix' THEN 'Fenix Nightly'
    WHEN date >= "2020-06-08" AND Package_Name =	'org.mozilla.firefox_beta' THEN 'Fenix Beta'
    WHEN Package_Name =	'org.mozilla.fenix.nightly' THEN 'Fenix Nightly'
    -- Other MAU Products
    WHEN Package_Name =	'org.mozilla.focus' THEN 'Focus Android'
    WHEN Package_Name =	'org.mozilla.rocket' THEN 'Firefox Lite'
    WHEN Package_Name =	'mozilla.lockbox' THEN 'Firefox Lockwise'
    WHEN Package_Name =	'org.mozilla.klar' THEN 'Firefox Klar'
    -- Other Products
    WHEN Package_Name =	'org.mozilla.testpilot.notes' THEN 'Notes By Firefox: A Secure Notepad App'
    WHEN Package_Name =	'org.mozilla.firefox.vpn' THEN 'Firefox Private Network VPN'
    WHEN Package_Name =	'org.mozilla.webmaker' THEN 'Webmaker'
    WHEN Package_Name =	'org.mozilla.screenshot.go' THEN 'Firefox ScreenshotGo Beta'
    WHEN Package_Name =	'org.mozilla.vrbrowser' THEN 'Firefox Reality Browser'
    WHEN Package_Name =	'org.mozilla.mozstumbler' THEN 'Mozilla Stumbler'
    WHEN Package_Name =	'org.mozilla.firefoxsend' THEN 'Firefox Send'
    WHEN Package_Name =	'org.mozilla.reference.browser' THEN 'Reference Browser'
    ELSE 'other'
  END as app_channel,
  os,
  acquisition_channel,
  CASE
    WHEN Acquisition_Channel IN ('Play Store (organic)', 'Google Search (organic)', 'Tracked Channel (UTM)') THEN 'unpaid'
    ELSE 'paid / other' END as acquisition_channel_type,
  SUM(store_listing_visitors) as store_listing_visitors,
  SUM(store_listing_visitors_7_day_avg) as store_listing_visitors_7_day_avg,
  SUM(installers) as installers,
  SUM(installers_7_day_avg) as installers_7_day_avg
FROM
  playstore_data
WHERE
  date <= (SELECT end_date FROM max_date)
  -- Limit to minimum max date between the two store data sources
  AND date <= (SELECT end_date FROM max_date)
GROUP BY 1,2,3,4,5

UNION ALL

-- Join for total Android Products
SELECT
  date,
  'All Android Products' as Package_Name,
  'All Android Products' as app_channel,
  os,
  acquisition_channel,
  CASE
    WHEN Acquisition_Channel IN ('Play Store (organic)', 'Google Search (organic)', 'Tracked Channel (UTM)') THEN 'unpaid'
    ELSE 'paid / other' END as acquisition_channel_type,
  SUM(store_listing_visitors) as store_listing_visitors,
  SUM(store_listing_visitors_7_day_avg) as store_listing_visitors_7_day_avg,
  SUM(installers) as installers,
  SUM(installers_7_day_avg) as installers_7_day_avg
FROM
  playstore_data
WHERE
  date <= (SELECT end_date FROM max_date)
  -- Selecting only products that are part of Non Desktop MAU to be included in All Android Products total chart
  AND Package_Name IN ('org.mozilla.firefox', 'org.mozilla.firefox_beta', 'org.mozilla.fennec_aurora', 'org.mozilla.focus', 'org.mozilla.fenix', 'org.mozilla.fenix.nightly', 'org.mozilla.rocket', 'mozilla.lockbox', 'org.mozilla.klar')
GROUP BY 1,2,3,4,5,6

UNION ALL

-- iOS products by product
SELECT
  date,
  Browser as package_name,
  browser as app_channel,
  os,
  source,
  CASE
    WHEN source IN ('App Store Browse', 'App Store Search', 'Web Referrer') THEN 'unpaid'
    ELSE 'paid / other' END as acquisition_channel_type,
  SUM(product_page_views_unique_device) as page_views_unique_device,
  SUM(pageviews_unique_device_7_day_avg) as pageviews_unique_device_7_day_avg,
  SUM(app_units) as installs,
  SUM(app_units_7_day_avg) as installs_7_day_avg
FROM
  apple_data
WHERE
  date <= (SELECT end_date FROM max_date)
GROUP BY 1,2,3,4,5,6

UNION ALL

-- Join total iOS Products
SELECT
  date,
  'All iOS Products' as package_name,
  'All iOS Products' as app_channel,
  os,
  source,
  CASE
    WHEN source IN ('App Store Browse', 'App Store Search', 'Web Referrer') THEN 'unpaid'
    ELSE 'paid / other' END as acquisition_channel_type,
  SUM(product_page_views_unique_device) as page_views_unique_device,
  SUM(pageviews_unique_device_7_day_avg) as pageviews_unique_device_7_day_avg,
  SUM(app_units) as installs,
  SUM(app_units_7_day_avg) as app_units_7_day_avg
FROM
  apple_data
WHERE
  date <= (SELECT end_date FROM max_date)
  -- Selecting only products that are part of Non Desktop MAU to be included in All iOS Products total chart
  AND browser IN ('Fennec iOS', 'Focus iOS', 'Firefox Lockwise iOS', 'Firefox Klar iOS')
GROUP BY 1,2,3,4,5,6