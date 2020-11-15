-- https://datastudio.google.com/reporting/1L7dsFyqjT8XZHrYprYS-HCP5_k_gZGIb/page/fujVB/edit

WITH storefront_data as (
SELECT
  * EXCEPT (active_devices_opt_in, active_devices_last_30_days_opt_in, deletions_opt_in, installations_opt_in, sessions_opt_in, opt_in_rate)
FROM
  `moz-fx-data-marketing-prod.apple_app_store.metrics_by_storefront`)

SELECT
  *,
  ROUND(AVG(impressions) OVER (PARTITION BY app_name, storefront ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS impressions_7_day_avg,
  ROUND(AVG(impressions_unique_device) OVER (PARTITION BY app_name, storefront ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS impressions_unique_device_7_day_avg,
  ROUND(AVG(product_page_views) OVER (PARTITION BY app_name, storefront ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS product_pageviews_7_day_avg,
  ROUND(AVG(product_page_views_unique_device) OVER (PARTITION BY app_name, storefront ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS product_pageviews_unique_device_7_day_avg,
  ROUND(AVG(app_units) OVER (PARTITION BY app_name, storefront ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS app_units_7_day_avg
FROM
  storefront_data