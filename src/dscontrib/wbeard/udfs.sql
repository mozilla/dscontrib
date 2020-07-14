CREATE TEMP FUNCTION median(arr Array<float64>) AS (
  (select approx_quantiles(reps, 2)[offset(1)] from unnest(arr) as reps)
);

CREATE TEMP FUNCTION median_exact(arr ANY TYPE) AS (
  (select percentile_disc(reps, .5) over() from unnest(arr) as reps limit 1)
);

CREATE TEMP FUNCTION geo_mean(xs any type) AS (
  -- when aggregating, use as `geo_mean(array_agg(i))`
  ((select exp(sum(ln(x)) / array_length(xs)) from unnest(xs) as x))
);

CREATE TEMP FUNCTION major_vers(st string) AS (
  -- '10.0' => 10
  cast(regexp_extract(st, '(\\d+)\\.?') as int64)
);