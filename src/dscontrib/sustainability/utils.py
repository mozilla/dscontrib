import pandas as pd
import pyspark.sql.functions as F


def bootstrap(df, iterations, reporting_freq=10, strata=[],
              treatment='variant', control='control'):
    def munge(s):
        agg = s.groupby(["experiment_branch"] + strata).mean()
        agg = (
          pd.melt(agg.reset_index(), id_vars=["experiment_branch"] + strata)
          .pivot_table(
                    index=strata + ['variable'],
                    columns='experiment_branch',
                    values='value').reset_index()
          )
        agg['change'] = (agg[treatment] - agg[control]) / agg[control]
        return agg

    n = len(df)
    results = munge(df)
    results['iteration'] = -1  # true sample stats lie at -1
    for i in range(iterations):
        if i % 10 == 0:
            print(i, end=' ')
        s = df.sample(n, replace=True)
        sagg = munge(s)
        sagg['iteration'] = i
        results = pd.concat([sagg, results])
    return results


measures = {
  'google_sap': 500,
  'sap': 500,
  'google_organic': 500,
  'organic': 500,
  'clicks': 100,
  'impressions': 300,
  'tab_open_event_count': 500,
  'uri_count': 1000
}


def cap_daily_measures(df, measures):
    for m in measures:
        col = F.col(m)
        cap = measures[m]
        df = df.withColumn(m, F.when(col > cap, cap).otherwise(col))
    return df


def percent_capped(df, measures):
    return(df
           .agg(*[(F.avg(
                  F.when(F.col(m) == measures[m], 1)
                   .otherwise(0))).alias(m)
                  for m in measures])
           ).toPandas().T
