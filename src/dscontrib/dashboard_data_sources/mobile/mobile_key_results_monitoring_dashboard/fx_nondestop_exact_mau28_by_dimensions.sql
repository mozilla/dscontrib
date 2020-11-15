WITH data as (
SELECT
  *
FROM
`moz-fx-data-derived-datasets.telemetry.firefox_nondesktop_exact_mau28_by_dimensions_v1`)

SELECT
  *,
  ROUND(AVG(dau) OVER (PARTITION BY product, normalized_channel, campaign, country, distribution_id, country_name, id_bucket ORDER BY submission_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) as dau_7d_ma
FROM
  data