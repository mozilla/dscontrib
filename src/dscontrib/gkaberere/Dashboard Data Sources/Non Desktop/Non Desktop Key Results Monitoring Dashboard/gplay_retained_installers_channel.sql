-- Dashboard Link: https://datastudio.google.com/reporting/1L7dsFyqjT8XZHrYprYS-HCP5_k_gZGIb/page/0iERB

SELECT
  *,
  ROUND(AVG(store_listing_visitors) OVER (PARTITION BY Package_Name, Acquisition_Channel ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS store_visitors_7_day_avg,
  ROUND(AVG(installers) OVER (PARTITION BY Package_Name, Acquisition_Channel ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS installers_7_day_avg
FROM
  `moz-fx-data-marketing-prod.google_play_store.p_Retained_installers_channel_v1`