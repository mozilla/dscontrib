-- Dashboard Link: https://datastudio.google.com/reporting/1L7dsFyqjT8XZHrYprYS-HCP5_k_gZGIb/page/0iERB

WITH data as (
SELECT
  EXTRACT(YEAR FROM Date) as Year,
  Date as install_date,
  Package_Name,
  CASE
	  WHEN Package_Name =	'org.mozilla.fenix.nightly' THEN 'Firefox Preview Nightly For Developers'
    WHEN Package_Name =	'org.mozilla.testpilot.notes' THEN 'Notes By Firefox: A Secure Notepad App'
    WHEN Package_Name =	'org.mozilla.firefox.vpn' THEN 'Firefox Private Network VPN'
    WHEN Package_Name =	'org.mozilla.webmaker' THEN 'Webmaker'
    WHEN Package_Name =	'org.mozilla.screenshot.go' THEN 'Firefox ScreenshotGo Beta'
    WHEN Package_Name =	'org.mozilla.firefox_beta' THEN 'Firefox Browser Beta'
    WHEN Package_Name =	'mozilla.lockbox' THEN 'Firefox Lockwise'
    WHEN Package_Name =	'org.mozilla.rocket' THEN 'Firefox Lite'
    WHEN Package_Name =	'org.mozilla.fenix' THEN 'Firefox Preview'
    WHEN Package_Name =	'org.mozilla.focus' THEN 'Firefox Focus'
    WHEN Package_Name =	'org.mozilla.firefox' THEN 'Firefox Browser'
    WHEN Package_Name =	'org.mozilla.vrbrowser' THEN 'Firefox Reality Browser'
    WHEN Package_Name =	'org.mozilla.mozstumbler' THEN 'Mozilla Stumbler'
    WHEN Package_Name =	'org.mozilla.firefoxsend' THEN 'Firefox Send'
    WHEN Package_Name =	'org.mozilla.klar' THEN 'Firefox Klar'
    WHEN Package_Name =	'org.mozilla.fennec_aurora' THEN 'Firefox Nightly for Developers'
    WHEN Package_Name =	'org.mozilla.reference.browser' THEN 'Reference Browser'
    ELSE 'other'
  END as app_name,
  Acquisition_Channel,
  SUM(store_listing_visitors) as store_listing_visitors,
  SUM(Installers) as installers
FROM
  `moz-fx-data-marketing-prod.google_play_store.p_Retained_installers_channel_v1`
WHERE
  Date >= "2019-01-01"
GROUP BY 1,2,3,4,5),

summary as(
SELECT
  *,
  ROUND(AVG(store_listing_visitors) OVER (PARTITION BY Package_Name, Acquisition_Channel ORDER BY install_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS store_listing_visitors_7_day_avg,
  ROUND(AVG(installers) OVER (PARTITION BY Package_Name, Acquisition_Channel ORDER BY install_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS installers_7_day_avg,
  CASE WHEN Year = 2019 THEN DATE_ADD(install_date, INTERVAL 1 YEAR) ELSE install_date END as date_2020
FROM
  data
ORDER BY
  2)

SELECT
  date_2020 as install_date,
  Package_Name,
  app_name,
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
  summary
WHERE
  date_2020 >= "2020-01-01"
	-- To remove the leap year day for yoy comparison
	-- AND date_2020 != "2020-02-29"
  AND date_2020 <= (SELECT max(date) FROM `moz-fx-data-marketing-prod.google_play_store.p_Retained_installers_channel_v1`)
GROUP BY 1,2,3,4