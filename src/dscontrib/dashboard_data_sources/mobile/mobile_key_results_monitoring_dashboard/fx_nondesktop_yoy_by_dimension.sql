WITH data as (
SELECT
  * ,
  CASE WHEN EXTRACT(YEAR FROM submission_date) = EXTRACT(YEAR FROM CURRENT_DATE) THEN submission_date ELSE DATE_ADD(submission_date, INTERVAL 1 YEAR) END as current_year_yoy_date,
FROM
`moz-fx-data-derived-datasets.telemetry.firefox_nondesktop_exact_mau28_by_dimensions_v1`
WHERE
 submission_date != "2020-02-29"),

summary_data as (
SELECT
  current_year_yoy_date,
  product,
  normalized_channel,
  campaign,
  country,
  distribution_id,
  country_name,
  id_bucket,
  SUM(CASE WHEN EXTRACT(YEAR FROM submission_date) = EXTRACT(YEAR FROM CURRENT_DATE)  THEN mau ELSE 0 END) as current_year_mau,
  SUM(CASE WHEN EXTRACT(YEAR FROM submission_date) = EXTRACT(YEAR FROM CURRENT_DATE)-1 THEN mau ELSE 0 END) as prior_year_mau,
  SUM(CASE WHEN EXTRACT(YEAR FROM submission_date) = EXTRACT(YEAR FROM CURRENT_DATE)  THEN wau ELSE 0 END) as current_year_wau,
  SUM(CASE WHEN EXTRACT(YEAR FROM submission_date) = EXTRACT(YEAR FROM CURRENT_DATE)-1 THEN wau ELSE 0 END) as prior_year_wau,
  SUM(CASE WHEN EXTRACT(YEAR FROM submission_date) = EXTRACT(YEAR FROM CURRENT_DATE)  THEN dau ELSE 0 END) as current_year_dau,
  SUM(CASE WHEN EXTRACT(YEAR FROM submission_date) = EXTRACT(YEAR FROM CURRENT_DATE)-1 THEN dau ELSE 0 END) as prior_year_dau
FROM
  data
GROUP BY 1,2,3,4,5,6,7,8)

SELECT
  *,
  ROUND(AVG(current_year_dau) OVER (PARTITION BY product, normalized_channel, campaign, country, distribution_id, country_name, id_bucket ORDER BY current_year_yoy_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) as current_year_dau_7d_ma,
  ROUND(AVG(prior_year_dau) OVER (PARTITION BY product, normalized_channel, campaign, country, distribution_id, country_name, id_bucket ORDER BY current_year_yoy_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) as prior_year_dau_7d_ma
FROM
  summary_data
WHERE
	product != "Firefox Reality"