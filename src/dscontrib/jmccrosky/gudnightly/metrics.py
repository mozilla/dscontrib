# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql import Window
from pyspark.sql.types import IntegerType

from dscontrib.jmccrosky.gudnightly.utils import jackknifeCountCI, jackknifeMeanCI


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


def metricMAU(data, needed_dimension_variables, feature_col, sampling_multiplier):
    days = 28
    all_user_days = data.select("id").distinct().crossJoin(
        data.select("date").distinct()
    )

    data = data.select(
        ["id", "date", feature_col, "bucket"] + needed_dimension_variables
    ).filter(col(feature_col) == 1)

    data = data.alias("data")
    all_user_days = all_user_days.alias("all_user_days")

    # Augment activity table to include non-active days
    data = data.join(
        all_user_days,
        ['id', 'date'],
        'outer'
    ).withColumn(
        "n_", F.coalesce("data." + feature_col, lit(0))
    ).drop(
        feature_col
    ).withColumnRenamed(
        "n_", feature_col
    )

    if days > 1:
        # Aggregate active days over time for each profile-day
        windowSpec = Window.partitionBy(
            [data.id]
        ).orderBy(
            data.date
        ).rowsBetween(
            1-days, 0
        )

        data = data.withColumn(
            "n_", F.max(data[feature_col]).over(windowSpec) * sampling_multiplier
        ).drop(
            feature_col
        ).withColumnRenamed(
            "n_", feature_col
        )
    for v in needed_dimension_variables:
        data = data.withColumn(v, F.last(v, True).over(windowSpec))

    # Reduce to sum of active days per date
    data = data.filter(
        col(feature_col) > 0
    ).groupBy(
        ["date", "bucket"] + needed_dimension_variables
    ).sum(
        feature_col
    ).withColumnRenamed(
        'sum({})'.format(feature_col),
        feature_col
    )

    # Add all-bucket rows
    data = data.unionByName(
        data.groupBy(
            ["date"] + needed_dimension_variables
        ).agg(
            F.sum(feature_col).alias(feature_col)
        ).withColumn(
            'bucket', lit("ALL")
        )
    )
    return data


# This function won't work with gudnightly currently and is a utility for
# other analysis.
def metricActiveHoursPerWeekPerProfileDay(
    data, needed_dimension_variables, feature_col, sampling_multiplier, days=7,
    include_day_of_week=False
):
    all_user_days = data.select("id").distinct().crossJoin(
        data.select("date").distinct()
    )
    intermediate_table1 = data.filter(
        col(feature_col) > 0
    ).select(
        ["id", "date", feature_col]
    ).distinct(
    )

    intermediate_table1 = intermediate_table1.alias("intermediate_table")
    all_user_days = all_user_days.alias("all_user_days")

    # Augment activity table to include non-active days
    intermediate_table2 = intermediate_table1.join(
        all_user_days,
        ['id', 'date'],
        'outer'
    ).withColumn(
        "n_", F.coalesce("intermediate_table." + feature_col, lit(0))
    ).drop(
        feature_col
    ).withColumnRenamed(
        "n_", feature_col
    )

    if include_day_of_week:
        intermediate_table2 = intermediate_table2.withColumn(
            "weekday_hours_sum",
            F.when(
                F.date_format('date', 'u').cast(IntegerType()) <= 5,
                col("active_hours_sum")
            ).otherwise(0)
        ).withColumn(
            "weekend_hours_sum",
            F.when(
                F.date_format('date', 'u').cast(IntegerType()) >= 6,
                col("active_hours_sum")
            ).otherwise(0)
        )

    # Calculate active hours for each profile-day
    windowSpec = Window.partitionBy(
        [intermediate_table2.id]
    ).orderBy(
        intermediate_table2.date
    ).rowsBetween(
        1 - days, 0
    )

    active_hours_table = intermediate_table2.withColumn(
        "hours", F.sum(intermediate_table2["active_hours_sum"]).over(windowSpec)
    )
    if include_day_of_week:
        active_hours_table = active_hours_table.withColumn(
            "weekday_hours",
            F.sum(intermediate_table2["weekday_hours_sum"]).over(windowSpec)
        ).withColumn(
            "weekend_hours",
            F.sum(intermediate_table2["weekend_hours_sum"]).over(windowSpec)
        )
    return active_hours_table


def metricSumDimensionOverWeekPerProfileDay(
    data, needed_dimension_variables, feature_col, sampling_multiplier, days=7,
    include_day_of_week=False
):
    all_user_days = data.select("id").distinct().crossJoin(
        data.select("date").distinct()
    )
    intermediate_table1 = data.filter(
        col(feature_col) > 0
    ).select(
        ["id", "date", feature_col]
    ).distinct(
    )

    intermediate_table1 = intermediate_table1.alias("intermediate_table")
    all_user_days = all_user_days.alias("all_user_days")

    # Augment activity table to include non-active days
    intermediate_table2 = intermediate_table1.join(
        all_user_days,
        ['id', 'date'],
        'outer'
    ).withColumn(
        "n_", F.coalesce("intermediate_table." + feature_col, lit(0))
    ).drop(
        feature_col
    ).withColumnRenamed(
        "n_", feature_col
    )

    if include_day_of_week:
        intermediate_table2 = intermediate_table2.withColumn(
            "weekday_" + feature_col,
            F.when(
                F.date_format('date', 'u').cast(IntegerType()) <= 5,
                col(feature_col)
            ).otherwise(0)
        ).withColumn(
            "weekend_" + feature_col,
            F.when(
                F.date_format('date', 'u').cast(IntegerType()) >= 6,
                col(feature_col)
            ).otherwise(0)
        )

    # Calculate active hours for each profile-day
    windowSpec = Window.partitionBy(
        [intermediate_table2.id]
    ).orderBy(
        intermediate_table2.date
    ).rowsBetween(
        1 - days, 0
    )

    active_hours_table = intermediate_table2.withColumn(
        "_temp", F.sum(intermediate_table2[feature_col]).over(windowSpec)
    )
    if include_day_of_week:
        active_hours_table = active_hours_table.withColumn(
            "_temp_weekday",
            F.sum(intermediate_table2["weekday_" + feature_col]).over(windowSpec)
        ).withColumn(
            "_temp_weekend",
            F.sum(intermediate_table2["weekend_" + feature_col]).over(windowSpec)
        )
    return active_hours_table.drop(
        feature_col,
        "weekday_" + feature_col,
        "weekend_" + feature_col,
    ).withColumnRenamed(
        "_temp",
        feature_col
    ).withColumnRenamed(
        "_temp_weekday",
        "weekday_" + feature_col
    ).withColumnRenamed(
        "_temp_weekend",
        "weekend_" + feature_col
    )


def metricDaysPerWeekPerProfileDay(
    data, needed_dimension_variables, feature_col, sampling_multiplier, days=7,
    include_day_of_week=False
):
    all_user_days = data.select("id").distinct().crossJoin(
        data.select("date").distinct()
    )

    data = data.filter(
        col(feature_col) > 0
    ).select(
        ["id", "date", "bucket", feature_col] + needed_dimension_variables
    ).distinct(
    )

    data = data.alias("intermediate_table")
    all_user_days = all_user_days.alias("all_user_days")

    # Augment activity table to include non-active days
    intermediate_table2 = data.join(
        all_user_days,
        ['id', 'date'],
        'outer'
    ).withColumn(
        "n_", F.coalesce("intermediate_table." + feature_col, lit(0))
    ).drop(
        feature_col
    ).withColumnRenamed(
        "n_", feature_col
    )

    if include_day_of_week:
        intermediate_table2 = intermediate_table2.withColumn(
            feature_col + "_weekend",
            F.when(
                F.date_format('date', 'u').cast(IntegerType()) >= 6,
                col(feature_col)
            ).otherwise(
                0
            )
        ).withColumn(
            feature_col + "_weekday",
            F.when(
                F.date_format('date', 'u').cast(IntegerType()) <= 5,
                col(feature_col)
            ).otherwise(
                0
            )
        )

    # Calculate active days per week for each profile-day
    windowSpec = Window.partitionBy(
        [intermediate_table2.id]
    ).orderBy(
        intermediate_table2.date
    ).rowsBetween(
        1 - days, 0
    )

    intermediate_table3 = intermediate_table2.withColumn(
        "n_", F.sum(intermediate_table2[feature_col]).over(windowSpec)
    ).drop(
        feature_col
    ).withColumnRenamed(
        "n_", feature_col
    )
    if include_day_of_week:
        intermediate_table3 = intermediate_table3.withColumn(
            "n_", F.sum(intermediate_table2[feature_col + "_weekend"]).over(windowSpec)
        ).drop(
            feature_col + "_weekend"
        ).withColumnRenamed(
            "n_", feature_col + "_weekend"
        )

        intermediate_table3 = intermediate_table3.withColumn(
            "n_", F.sum(intermediate_table2[feature_col + "_weekday"]).over(windowSpec)
        ).drop(
            feature_col + "_weekday"
        ).withColumnRenamed(
            "n_", feature_col + "_weekday"
        )

    for v in needed_dimension_variables:
        intermediate_table3 = intermediate_table3.withColumn(
            v, F.last(v, True).over(windowSpec)
        )
    return intermediate_table3


def metricDaysPerWeek(
    data, needed_dimension_variables, feature_col, sampling_multiplier
):
    intermediate_table3 = metricDaysPerWeekPerProfileDay(
        data, needed_dimension_variables, feature_col, sampling_multiplier
    )

    # Reduce to mean active days per week per date
    intermediate_table4 = intermediate_table3.filter(
        col(feature_col) > 0
    ).groupBy(
        ["date", "bucket"] + needed_dimension_variables
    ).mean(
        feature_col
    ).withColumnRenamed(
        'avg({})'.format(feature_col),
        feature_col
    )
    intermediate_table4_allbucket = intermediate_table3.filter(
        col(feature_col) > 0
    ).groupBy(
        ["date"] + needed_dimension_variables
    ).mean(
        feature_col
    ).withColumnRenamed(
        'avg({})'.format(feature_col),
        feature_col
    ).withColumn(
        'bucket', lit("ALL")
    )

    # Add all-bucket rows
    joined_intermediate = intermediate_table4.unionByName(
        intermediate_table4_allbucket
    )
    return joined_intermediate


def metricRetention(
    data, needed_dimension_variables, feature_col, sampling_multiplier,
    activated=False
):
    activity_data = data.filter(
        col(feature_col) > 0
    ).select(
        ["id", "date", feature_col]
    ).distinct()

    pcd_table = data.select(
        ["date", "id", "bucket"] + needed_dimension_variables
    )
    windowSpec = Window.partitionBy(
        [pcd_table.id] + needed_dimension_variables
    ).orderBy(
        pcd_table.date
    ).rowsBetween(
        0, 13
    )
    for v in needed_dimension_variables:
        pcd_table = pcd_table.withColumn(v, F.last(v, True).over(windowSpec))
    pcd_table = pcd_table.filter(
        col("new_profile") == 1
    )

    if activated:
        pcd_table = pcd_table.alias("pcd_t").join(
            activity_data.alias("i_t"),
            (col('pcd_t.id') == col('i_t.id')) &
            (col('i_t.date') >= F.date_add(col('pcd_t.date'), 1)) &
            (col('i_t.date') <= F.date_add(col('pcd_t.date'), 6)),
            "inner"
        ).filter(
            col("i_t." + feature_col) > 0
        ).dropDuplicates(
            ['id']
        ).select(
            [
                col('pcd_t.{}'.format(c))
                for c in ['id', 'bucket', "date"] + needed_dimension_variables
            ]
        )

    intermediate_table3 = pcd_table.alias("pcd_t").join(
        activity_data.alias("i_t"),
        (col('pcd_t.id') == col('i_t.id')) &
        (col('i_t.date') >= F.date_add(col('pcd_t.date'), 7)) &
        (col('i_t.date') <= F.date_add(col('pcd_t.date'), 13)),
        "outer"
    ).select(
        [
            'pcd_t.{}'.format(c)
            for c in ['id', 'date', 'bucket'] +
            needed_dimension_variables
        ] + [feature_col]
    ).fillna(
        0, [feature_col]
    ).groupBy(
        [
            'pcd_t.{}'.format(c)
            for c in ['id', 'date', 'bucket'] +
            needed_dimension_variables
        ],
    ).agg(
        F.max(col(feature_col))
    ).drop(
        feature_col
    ).withColumnRenamed(
        "MAX({})".format(feature_col), feature_col
    ).select(
        [
            col("pcd_t.{}".format(c)).alias(c)
            for c in ['id', 'bucket', 'date'] +
            needed_dimension_variables
        ] + [feature_col]
    )

    intermediate_table4 = intermediate_table3.groupBy(
        ["date", "bucket"] + needed_dimension_variables
    ).mean(
        feature_col
    ).withColumnRenamed(
        'avg({})'.format(feature_col), feature_col
    )
    intermediate_table4_allbucket = intermediate_table3.groupBy(
        ["date"] + needed_dimension_variables
    ).mean(
        feature_col
    ).withColumnRenamed(
        'avg({})'.format(feature_col), feature_col
    ).withColumn(
        'bucket', lit("ALL")
    )

    joined_intermediate = intermediate_table4.unionByName(
        intermediate_table4_allbucket
    )
    return joined_intermediate


def metricActivatedRetention(
    data, needed_dimension_variables, feature_col, sampling_multiplier
):
    return metricRetention(
        data, needed_dimension_variables, feature_col, sampling_multiplier, True
    )


metricFunctions = {
    "DAU": metricDAU,
    "MAU": metricMAU,
    # "MAU31": metricMAU31,
    # "MAU27": metricMAU27,
    "Days Per Week": metricDaysPerWeek,
    "Week 1 Retention": metricRetention,
    "Week 1 Activated Retention": metricActivatedRetention,
    # "Week 1 Retention (excluding single-day profiles)": metricRealishRetention,
}

metricAggregations = {
    "DAU": lambda x: x.sum(),
    "MAU": lambda x: x.sum(),
    # "MAU31": lambda x: x.sum(),
    # "MAU27": lambda x: x.sum(),
    "Days Per Week": lambda x: x.sum() if len(x) == 1 else 0,
    "Week 1 Retention": lambda x: x.sum() if len(x) == 1 else 0,
    "Week 1 Activated Retention": lambda x: x.sum() if len(x) == 1 else 0,
    # "Week 1 Retention (excluding single-day profiles)":
    #      lambda x: x.sum() if len(x)==1 else 0,
}

metricDaysNeededPre = {
    "DAU": 0,
    "MAU": 27,
    # "MAU31": 30,
    # "MAU27": 26,
    "Days Per Week": 6,
    "Week 1 Retention": 0,
    "Week 1 Activated Retention": 0,
    # "Week 1 Retention (excluding single-day profiles)": 0,
}

metricDaysNeededPost = {
    "DAU": 0,
    "MAU": 0,
    # "MAU31": 0,
    # "MAU27": 0,
    "Days Per Week": 0,
    "Week 1 Retention": 13,
    "Week 1 Activated Retention": 13,
    # "Week 1 Retention (excluding single-day profiles)": 13,
}

metricCIs = {
    "DAU": jackknifeCountCI,
    # "DAU-alt": poissonCI,
    "MAU": jackknifeCountCI,
    # "MAU31": jackknifeCountCI,
    # "MAU27": jackknifeCountCI,
    # "Retention-debug": jackknifeCountCI,
    "Days Per Week": jackknifeMeanCI,
    "Week 1 Retention": jackknifeMeanCI,
    "Week 1 Activated Retention": jackknifeMeanCI,
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
