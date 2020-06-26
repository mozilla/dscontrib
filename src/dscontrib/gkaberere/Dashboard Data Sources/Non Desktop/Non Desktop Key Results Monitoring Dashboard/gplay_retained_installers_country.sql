-- Dashboard Link: https://datastudio.google.com/reporting/1L7dsFyqjT8XZHrYprYS-HCP5_k_gZGIb/page/0iERB

WITH country_data as (
SELECT
  *,
  ROUND(AVG(store_listing_visitors) OVER (PARTITION BY Package_Name, Country ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS store_visitors_7_day_avg,
  ROUND(AVG(installers) OVER (PARTITION BY Package_Name, Country ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS installers_7_day_avg
FROM
  `moz-fx-data-marketing-prod.google_play_store.p_Retained_installers_country_v1`),

country_names as (
SELECT
  *
FROM
  `moz-fx-data-derived-datasets.analysis.standardized_country_list` )
SELECT
  *
FROM
  country_data
LEFT JOIN
  country_names
ON
  LOWER(country_data.Country) = country_names.rawCountry