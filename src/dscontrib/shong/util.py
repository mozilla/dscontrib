# date utils:

# date_plus_N(date, N):
# date_to_string(date, format='%Y%m%d'):
# string_to_date(date, format='%Y%m%d'):

# spark utils

# read_parquet_from_s3(path, spark=spark):
# write_parquet_to_s3(df, path, mode='overwrite', spark=spark):
# read_main_summary():
# register_df(df, name, cache=False, sqlContext=sqlContext):


# date util: imports
import datetime
from datetime import timedelta

# spark util: imports
from pyspark.sql import SparkSession

# --------------- date utils ---------------


def date_plus_N(date, N):
    """
    takes datetime.date
    return another datetime.date N days away
    """
    return date + timedelta(days=N)


def date_to_string(date, format='%Y%m%d'):
    """
    takes datetime.date
    returns it back in string form
    """
    return date.strftime(format)


def string_to_date(date, format='%Y%m%d'):
    """
    takes string
    returns datetime.date
    """
    return datetime.datetime.strptime(date, format).date()


# --------------- spark utils ---------------


spark = SparkSession.builder.getOrCreate()


def read_parquet_from_s3(path):
    """
    read a df from s3 source
        returns df
    """
    return spark.read.parquet(path)


def write_parquet_to_s3(df, path, mode='overwrite'):
    """
    write a df to s3 source
    """
    df.write.mode(mode).parquet(path)
    print('data saved to: %s' % path)


def read_main_summary():
    """
    load ms directly from s3
        returns df
    """
    ms_path = 's3://telemetry-parquet/main_summary/v4/'
    return spark.read.option("mergeSchema", "true").parquet(ms_path)
