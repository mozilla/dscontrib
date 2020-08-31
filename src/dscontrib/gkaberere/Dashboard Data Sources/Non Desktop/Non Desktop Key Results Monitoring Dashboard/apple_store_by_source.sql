-- https://datastudio.google.com/reporting/1L7dsFyqjT8XZHrYprYS-HCP5_k_gZGIb/page/fujVB/edit

WITH source_data as (
SELECT
  *
FROM
  `moz-fx-data-marketing-prod.apple_app_store.metrics_by_source`)

SELECT
  *,
  ROUND(AVG(impressions) OVER (PARTITION BY app_name, source ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS impressions_7_day_avg,
  ROUND(AVG(impressions_unique_device) OVER (PARTITION BY app_name, source ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS impressions_unique_device_7_day_avg,
  ROUND(AVG(product_page_views) OVER (PARTITION BY app_name, source ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS product_pageviews_7_day_avg,
  ROUND(AVG(product_page_views_unique_device) OVER (PARTITION BY app_name, source ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS product_pageviews_unique_device_7_day_avg,
  ROUND(AVG(app_units) OVER (PARTITION BY app_name, source ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS app_units_7_day_avg
FROM
  source_data