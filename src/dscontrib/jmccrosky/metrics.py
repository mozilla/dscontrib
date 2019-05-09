# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from pyspark.sql import functions as F

from dscontrib.jmccrosky.utils import jackknifeCountCI


def metricDAU(data, needed_dimension_variables, feature_col, sampling_multiplier):
    data = data.select(
        ["date", feature_col, "bucket"] + needed_dimension_variables
    ).groupBy(
        ["date", "bucket"] + needed_dimension_variables
    ).agg(
        (F.sum(feature_col) * sampling_multiplier).alias(feature_col)
    )

    data = data.unionByName(
        data.groupBy(
            ["date"] + needed_dimension_variables
        ).agg(
            F.sum(feature_col).alias(feature_col)
        ).withColumn(
            'bucket', F.lit("ALL")
        )
    )
    return data


metricFunctions = {
  "DAU": metricDAU,
  # "MAU": metricMAU,
  # "MAU31": metricMAU31,
  # "MAU27": metricMAU27,
  # "Days Per Week": metricDaysPerWeek,
  # "Week 1 Retention": metricRetention,
  # "Week 1 Retention (excluding single-day profiles)": metricRealishRetention,
}

metricAggregations = {
  "DAU": lambda x: x.sum(),
  # "MAU": lambda x: x.sum(),
  # "MAU31": lambda x: x.sum(),
  # "MAU27": lambda x: x.sum(),
  # "Days Per Week": lambda x: x.sum() if len(x)==1 else 0,
  # "Week 1 Retention": lambda x: x.sum() if len(x)==1 else 0,
  # "Week 1 Retention (excluding single-day profiles)":
  #      lambda x: x.sum() if len(x)==1 else 0,
}

metricDaysNeededPre = {
  "DAU": 0,
  # "MAU": 27,
  # "MAU31": 30,
  # "MAU27": 26,
  # "Days Per Week": 6,
  # "Week 1 Retention": 0,
  # "Week 1 Retention (excluding single-day profiles)": 0,
}

metricDaysNeededPost = {
  "DAU": 0,
  # "MAU": 0,
  # "MAU31": 0,
  # "MAU27": 0,
  # "Days Per Week": 0,
  # "Week 1 Retention": 13,
  # "Week 1 Retention (excluding single-day profiles)": 13,
}

metricCIs = {
  "DAU": jackknifeCountCI,
  # "DAU-alt": poissonCI,
  # "MAU": jackknifeCountCI,
  # "MAU31": jackknifeCountCI,
  # "MAU27": jackknifeCountCI,
  # "Retention-debug": jackknifeCountCI,
  # "Days Per Week": jackknifeMeanCI,
  # "Week 1 Retention": jackknifeMeanCI,
  # "Week 1 Retention-alt": binomialCI,
  # "Week 1 Retention (excluding single-day profiles)": jackknifeMeanCI,
  # "Retention-filter": jackknifeMeanCI,
  # "WAU": jackknifeCountCI,
}

metricHTs = {
  # "DAU": permutationTestCount,
  # "Days Per Week": permutationTestMean,
  # "MAU": permutationTestCount,
}
