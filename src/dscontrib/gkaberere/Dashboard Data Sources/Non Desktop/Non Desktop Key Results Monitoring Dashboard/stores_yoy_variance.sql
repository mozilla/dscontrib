-- https://datastudio.google.com/reporting/1L7dsFyqjT8XZHrYprYS-HCP5_k_gZGIb/page/fujVB/edit

-- Query to power yoy tracking of unique pageviews for the app stores, a 2020 KR
WITH playstore_data as (
-- Pull all playstore data by source
SELECT
  EXTRACT(YEAR FROM Date) as Year,
  Date as install_date,
  Package_Name,
  Acquisition_Channel,
  store_listing_visitors,
  Installers,
  -- 7 day averaging for easier to read visualizations
  ROUND(AVG(store_listing_visitors) OVER (PARTITION BY Package_Name, Acquisition_Channel ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS store_listing_visitors_7_day_avg,
  ROUND(AVG(installers) OVER (PARTITION BY Package_Name, Acquisition_Channel ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS installers_7_day_avg,
  -- Convert 2019 dates to 2020 date to enable YoY change calculation
  CASE WHEN EXTRACT(Year FROM date)= 2019 THEN DATE_ADD(date, INTERVAL 1 YEAR) ELSE date END as date_2020
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
  -- 7 day averaging for easier to read visualizations
  ROUND(AVG(impressions) OVER (PARTITION BY app_name, source ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS impressions_7_day_avg,
  ROUND(AVG(impressions_unique_device) OVER (PARTITION BY app_name, source ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS impressions_unique_device_7_day_avg,
  ROUND(AVG(product_page_views) OVER (PARTITION BY app_name, source ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS pageviews_7_day_avg,
  ROUND(AVG(product_page_views_unique_device) OVER (PARTITION BY app_name, source ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS pageviews_unique_device_7_day_avg,
  ROUND(AVG(app_units) OVER (PARTITION BY app_name, source ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS app_units_7_day_avg,
  EXTRACT(YEAR FROM Date) as year,
  -- Convert 2019 dates to 2020 date to enable YoY change calculation
  CASE WHEN EXTRACT(Year FROM date) = 2019 THEN DATE_ADD(Date, INTERVAL 1 YEAR) ELSE Date END as date_2020
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
  date_2020 as install_date,
  Package_Name,
  CASE
    -- Fennec Android
    -- 2020-06-08 was when Beta channel was fully migrated to Fenix
    WHEN Package_Name =	'org.mozilla.firefox' THEN 'Fennec Android'
    WHEN install_date < "2020-06-08" AND Package_Name =	'org.mozilla.firefox_beta' THEN 'Fennec Android Beta'
    WHEN Package_Name =	'org.mozilla.fennec_aurora' THEN 'Fennec Android Nightly'
    -- Fenix
    WHEN install_date < "2020-06-08" AND Package_Name =	'org.mozilla.fenix' THEN 'Fenix'
    WHEN install_date >= "2020-06-08" AND Package_Name =	'org.mozilla.fenix' THEN 'Fenix Nightly'
    WHEN install_date >= "2020-06-08" AND Package_Name =	'org.mozilla.firefox_beta' THEN 'Fenix Beta'
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
  Acquisition_Channel,
  SUM(CASE WHEN year = 2019 THEN store_listing_visitors ELSE 0 END) as store_listing_visitors_2019,
  SUM(CASE WHEN year = 2020 THEN store_listing_visitors ELSE 0 END) as store_listing_visitors_2020,
  SUM(CASE WHEN year = 2019 THEN installers ELSE 0 END) as installers_2019,
  SUM(CASE WHEN year = 2020 THEN installers ELSE 0 END) as installers_2020,
  SUM(CASE WHEN year = 2019 THEN store_listing_visitors_7_day_avg ELSE 0 END) as store_listing_visitors_7_day_avg_2019,
  SUM(CASE WHEN year = 2020 THEN store_listing_visitors_7_day_avg ELSE 0 END) as store_listing_visitors_7_day_avg_2020,
  SUM(CASE WHEN year = 2019 THEN installers_7_day_avg ELSE 0 END) as installers_7_day_avg_2019,
  SUM(CASE WHEN year = 2020 THEN installers_7_day_avg ELSE 0 END) as installers_7_day_avg_2020
FROM
  playstore_data
WHERE
  date_2020 >= "2020-01-01"
	-- To remove the leap year day for yoy comparison
	/*AND date_2020 != "2020-02-29"*/
  -- Limit to minimum max date between the two store data sources
  AND date_2020 <= (SELECT end_date FROM max_date)
GROUP BY 1,2,3,4

UNION ALL

-- Join for total Android Products
SELECT
  date_2020 as install_date,
  'All Android Products' as Package_Name,
  'All Android Products' as app_channel,
  Acquisition_Channel,
  SUM(CASE WHEN year = 2019 THEN store_listing_visitors ELSE 0 END) as store_listing_visitors_2019,
  SUM(CASE WHEN year = 2020 THEN store_listing_visitors ELSE 0 END) as store_listing_visitors_2020,
  SUM(CASE WHEN year = 2019 THEN installers ELSE 0 END) as installers_2019,
  SUM(CASE WHEN year = 2020 THEN installers ELSE 0 END) as installers_2020,
  SUM(CASE WHEN year = 2019 THEN store_listing_visitors_7_day_avg ELSE 0 END) as store_listing_visitors_7_day_avg_2019,
  SUM(CASE WHEN year = 2020 THEN store_listing_visitors_7_day_avg ELSE 0 END) as store_listing_visitors_7_day_avg_2020,
  SUM(CASE WHEN year = 2019 THEN installers_7_day_avg ELSE 0 END) as installers_7_day_avg_2019,
  SUM(CASE WHEN year = 2020 THEN installers_7_day_avg ELSE 0 END) as installers_7_day_avg_2020
FROM
  playstore_data
WHERE
  date_2020 >= "2020-01-01"
	-- To remove the leap year day for yoy comparison
	/*AND date_2020 != "2020-02-29"*/
  -- Limit to minimum max date between the two store data sources
  AND date_2020 <= (SELECT end_date FROM max_date)
  -- Selecting only products that are part of Non Desktop MAU to be included in All Android Products total chart
  AND Package_Name IN ('org.mozilla.firefox', 'org.mozilla.firefox_beta', 'org.mozilla.fennec_aurora', 'org.mozilla.focus', 'org.mozilla.fenix', 'org.mozilla.fenix.nightly', 'org.mozilla.rocket', 'mozilla.lockbox', 'org.mozilla.klar')
GROUP BY 1,2,3,4

UNION ALL

-- iOS products by product
SELECT
  date_2020 as install_date,
  Browser as package_name,
  browser as app_channel,
  source,
  SUM(CASE WHEN YEAR = 2019 THEN product_page_views_unique_device ELSE 0 END) as store_listing_visitors_2019,
  SUM(CASE WHEN YEAR = 2020 THEN product_page_views_unique_device ELSE 0 END) as store_listing_visitors_2020,
  NULL as installers_2019,
  NULL as installers_2020,
  SUM(CASE WHEN YEAR = 2019 THEN pageviews_unique_device_7_day_avg ELSE 0 END) as store_listing_visitors_7_day_avg_2019,
  SUM(CASE WHEN YEAR = 2020 THEN pageviews_unique_device_7_day_avg ELSE 0 END) as store_listing_visitors_7_day_avg_2020,
  NULL as installers_7_day_avg_2019,
  NULL as installers_7_day_avg_2020,
FROM
  apple_data
WHERE
  date_2020 >= "2020-01-01"
  /*AND date_2020 != "2020-02-29"*/
  AND date_2020 <= (SELECT end_date FROM max_date)
GROUP BY 1,2,3,4

UNION ALL

-- Join total iOS Products
SELECT
  date_2020 as install_date,
  'All iOS Products' as package_name,
  'All iOS Products' as app_channel,
  source,
  SUM(CASE WHEN YEAR = 2019 THEN product_page_views_unique_device ELSE 0 END) as store_listing_visitors_2019,
  SUM(CASE WHEN YEAR = 2020 THEN product_page_views_unique_device ELSE 0 END) as store_listing_visitors_2020,
  NULL as installers_2019,
  NULL as installers_2020,
  SUM(CASE WHEN YEAR = 2019 THEN pageviews_unique_device_7_day_avg ELSE 0 END) as store_listing_visitors_7_day_avg_2019,
  SUM(CASE WHEN YEAR = 2020 THEN pageviews_unique_device_7_day_avg ELSE 0 END) as store_listing_visitors_7_day_avg_2020,
  NULL as installers_7_day_avg_2019,
  NULL as installers_7_day_avg_2020
FROM
  apple_data
WHERE
  date_2020 >= "2020-01-01"
  -- To remove the leap year day for yoy comparison
  /*AND date_2020 != "2020-02-29"*/
  -- Limit to minimum max date between the two store data sources
  AND date_2020 <= (SELECT end_date FROM max_date)
  -- Selecting only products that are part of Non Desktop MAU to be included in All iOS Products total chart
  AND browser IN ('Fennec iOS', 'Focus iOS', 'Firefox Lockwise iOS', 'Firefox Klar iOS')
GROUP BY 1,2,3,4

UNION ALL
-- Join All iOS products as All Products to get an All Products Total
SELECT
  date_2020 as install_date,
  'All Products' as package_name,
  'All Products' as app_channel,
  source,
  SUM(CASE WHEN YEAR = 2019 THEN product_page_views_unique_device ELSE 0 END) as store_listing_visitors_2019,
  SUM(CASE WHEN YEAR = 2020 THEN product_page_views_unique_device ELSE 0 END) as store_listing_visitors_2020,
  NULL as installers_2019,
  NULL as installers_2020,
  SUM(CASE WHEN YEAR = 2019 THEN pageviews_unique_device_7_day_avg ELSE 0 END) as store_listing_visitors_7_day_avg_2019,
  SUM(CASE WHEN YEAR = 2020 THEN pageviews_unique_device_7_day_avg ELSE 0 END) as store_listing_visitors_7_day_avg_2020,
  NULL as installers_7_day_avg_2019,
  NULL as installers_7_day_avg_2020
FROM
  apple_data
WHERE
  date_2020 >= "2020-01-01"
  -- To remove the leap year day for yoy comparison
  /*AND date_2020 != "2020-02-29"*/
  -- Limit to minimum max date between the two store data sources
  AND date_2020 <= (SELECT end_date FROM max_date)
  -- Selecting only products that are part of Non Desktop MAU to be included in All iOS Products total chart
  AND browser IN ('Fennec iOS', 'Focus iOS', 'Firefox Lockwise iOS', 'Firefox Klar iOS')
GROUP BY 1,2,3,4

UNION ALL

SELECT
  date_2020 as install_date,
  'All Products' as Package_Name,
  'All Products' as app_channel,
  Acquisition_Channel,
  SUM(CASE WHEN year = 2019 THEN store_listing_visitors ELSE 0 END) as store_listing_visitors_2019,
  SUM(CASE WHEN year = 2020 THEN store_listing_visitors ELSE 0 END) as store_listing_visitors_2020,
  SUM(CASE WHEN year = 2019 THEN installers ELSE 0 END) as installers_2019,
  SUM(CASE WHEN year = 2020 THEN installers ELSE 0 END) as installers_2020,
  SUM(CASE WHEN year = 2019 THEN store_listing_visitors_7_day_avg ELSE 0 END) as store_listing_visitors_7_day_avg_2019,
  SUM(CASE WHEN year = 2020 THEN store_listing_visitors_7_day_avg ELSE 0 END) as store_listing_visitors_7_day_avg_2020,
  SUM(CASE WHEN year = 2019 THEN installers_7_day_avg ELSE 0 END) as installers_7_day_avg_2019,
  SUM(CASE WHEN year = 2020 THEN installers_7_day_avg ELSE 0 END) as installers_7_day_avg_2020
FROM
  playstore_data
WHERE
  date_2020 >= "2020-01-01"
	-- To remove the leap year day for yoy comparison
	/*AND date_2020 != "2020-02-29"*/
  -- Limit to minimum max date between the two store data sources
  AND date_2020 <= (SELECT end_date FROM max_date)
  -- Selecting only products that are part of Non Desktop MAU to be included in All Android Products total chart
  AND Package_Name IN ('org.mozilla.firefox', 'org.mozilla.firefox_beta', 'org.mozilla.fennec_aurora', 'org.mozilla.focus', 'org.mozilla.fenix', 'org.mozilla.fenix.nightly', 'org.mozilla.rocket', 'mozilla.lockbox', 'org.mozilla.klar')
GROUP BY 1,2,3,4