-- https://datastudio.google.com/reporting/1L7dsFyqjT8XZHrYprYS-HCP5_k_gZGIb/page/fujVB/edit

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
  *,
  CASE
	  -- Fennec Android
	-- 2020-06-08 was when Beta channel was fully migrated to Fenix
    WHEN Package_Name =	'org.mozilla.firefox' THEN 'Fennec Android Release'
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
    -- Other Products
    WHEN Package_Name =	'org.mozilla.klar' THEN 'Firefox Klar'
    WHEN Package_Name =	'org.mozilla.testpilot.notes' THEN 'Notes By Firefox: A Secure Notepad App'
    WHEN Package_Name =	'org.mozilla.firefox.vpn' THEN 'Firefox Private Network VPN'
    WHEN Package_Name =	'org.mozilla.webmaker' THEN 'Webmaker'
    WHEN Package_Name =	'org.mozilla.screenshot.go' THEN 'Firefox ScreenshotGo Beta'
    WHEN Package_Name =	'org.mozilla.vrbrowser' THEN 'Firefox Reality Browser'
    WHEN Package_Name =	'org.mozilla.mozstumbler' THEN 'Mozilla Stumbler'
    WHEN Package_Name =	'org.mozilla.firefoxsend' THEN 'Firefox Send'
    WHEN Package_Name =	'org.mozilla.reference.browser' THEN 'Reference Browser'
    ELSE 'other'
  END as app_channel
FROM
  country_data
LEFT JOIN
  country_names
ON
  LOWER(country_data.Country) = country_names.rawCountry