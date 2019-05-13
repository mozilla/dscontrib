# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql import Window

# Source table
_TABLE_SOURCE = "clients_daily"

# Source Columns
_COL_ID = col("client_id")
_COL_DATE = col("submission_date_s3")
_COL_URI_COUNT = F.col("scalar_parent_browser_engagement_total_uri_count_sum")
_COL_PC_DATE = col("profile_creation_date")
_COL_SAMPLE = col("sample_id")

# Other constants
_NUM_ADAU_THRESHOLD = 5
_DATE_PARSED = F.to_date(_COL_DATE, 'yyyyMMdd')
_MAP_NATURAL_DIMENSIONS = {
    "country": "country",
    "channel": "channel",
    "os": "os",
    "os_version": "os_version",
    "normalized_channel": "normalized_channel",
    "active_hours_sum": "active_hours_sum",
    "search_count_all": "search_count_all",
    "distribution_id": "distribution_id",
}

# Data file columns (TODO)


def createDataFile(
    start_date, end_date, spark_instance, jackknife_buckets, sample_percent, output_path
):
    feature_data_phase1 = spark_instance.table(_TABLE_SOURCE).select(
        [
            _COL_ID.alias("id"),
            _DATE_PARSED.alias("date"),
            # TODO: Use MD5 instead of CRC32
            ((F.crc32(_COL_ID) / 100) % jackknife_buckets).alias("bucket"),
            lit(1).alias("is_active"),
            F.when(
                _COL_URI_COUNT >= _NUM_ADAU_THRESHOLD, 1
            ).otherwise(
                0
            ).alias("is_active_active"),
            F.to_date(_COL_PC_DATE).alias("profile_creation_date")
        ] + _MAP_NATURAL_DIMENSIONS.keys()
    ).filter(
        (_DATE_PARSED.between(start_date, end_date)) &
        (_COL_SAMPLE < sample_percent)
    ).withColumn(
        "young_profile",
        F.when(
            col("date") < F.date_add(col("profile_creation_date"), 14), "TRUE"
        ).otherwise(
            "FALSE"
        )
    )

    new_profile_window = Window.partitionBy(col("id")).orderBy(col("date"))
    new_profile_data = feature_data_phase1.filter(
        (col("date") >= col("profile_creation_date")) &
        (col("date") <= F.date_add(col("profile_creation_date"), 6))
    ).select(
        "*",
        F.rank().over(new_profile_window).alias('rank')
    ).filter(
        col('rank') == 1
    ).withColumn(
        "new_profile", lit(1)
    ).drop(
        "date"
    ).withColumn(
        "date", col("profile_creation_date")
    )

    feature_data = feature_data_phase1.alias("fd").join(
        new_profile_data.alias("np"),
        (col("fd.id") == col("np.id")) & (col("fd.date") == col("np.date")),
        how='full',
    ).select(
        [F.coalesce(col("np.new_profile"), lit(0)).alias("new_profile")] +
        [F.coalesce(col("fd.is_active"), lit(0)).alias("is_active")] +
        [F.coalesce(col("fd.is_active_active"), lit(0)).alias("is_active_active")] +
        [
            F.coalesce(col("fd.{}".format(c)), col("np.{}".format(c))).alias(c)
            for c in feature_data_phase1.columns
            if c not in ["is_active", "is_active_active"]
        ]
    )

    once_ever_profiles = feature_data.filter(col("is_active") == 1).groupBy(
        "id"
    ).count(
    ).filter(
        col("count") == 1
    ).select(
        "id"
    ).withColumn(
        "single_day_profile", lit("1")
    )

    feature_data = feature_data.alias("fd").join(
        once_ever_profiles.alias("oep"),
        "id",
        "outer"
    ).fillna(
        {"single_day_profile": "0"}
    )

    feature_data.write.partitionBy("date").mode('overwrite').parquet(output_path)


def readDataFile(spark_instance, data_path):
    return spark_instance.read.parquet(data_path)
