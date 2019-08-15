# activity stream utils

# pull_tiles_data(sql_query, dbutils)
# validate_as_data_quality(sql_query, dates, dbutils)


# activity stream utils: imports
from pyspark.sql import SparkSession
import socket
# local dependences
from shong.util import write_parquet_to_s3, read_parquet_from_s3, date_to_string
from shong.constants import S3_ROOT

# --------------- activity stream utils ---------------


spark = SparkSession.builder.getOrCreate()


def pull_tiles_data(sql_query, dbutils):
    """
    provide SQL query, will return spark df of results
    """

    def tiles_redshift_jdbcurl_fetch(dbutils=dbutils): # noqa
        socket.setdefaulttimeout(3)
        s = socket.socket()
        rs = 'databricks-tiles-redshift.data.mozaws.net:5432'
        hostname, port = rs.split(":")
        port = int(port)
        address = socket.gethostbyname(hostname)
        print(address)
        try:
            s.connect((address, port))
            print("connected")
        except Exception as e:
            print("something's wrong with %s:%d. Exception is %s" % (address, port, e))
        finally:
            s.close()
        jdbcurl = ("jdbc:postgresql://{0}:{1}" +
                   "/tiles?user={2}" +
                   "&password={3}&ssl=true" +
                   "&sslMode=verify-ca"
                   ).format(hostname,
                            port,
                            dbutils.secrets.get('tiles-redshift', 'username'),
                            dbutils.secrets.get('tiles-redshift', 'password'))
        return jdbcurl

    TEMPDIR = "s3n://mozilla-databricks-telemetry-test/tiles-redshift/_temp"
    JDBC_URL = tiles_redshift_jdbcurl_fetch()

    df = spark.read.format("com.databricks.spark.redshift")\
                   .option("forward_spark_s3_credentials", True)\
                   .option("url", JDBC_URL).option("tempdir", TEMPDIR)\
                   .option("query", sql_query)\
                   .load()
    return df


def validate_as_data_quality(sql_query, dates, dbutils):
    """
    for a given query and range/list of dates, check if
    databricks can pull the data into parquet at all
    most likely outcomes are:
        1) data pulls correctly (the counts are correct)
        2) spark can't communicate with the redshift db
           at which point you just wait / switch clusters /
           try again.
        3) pull fails and there will be a long py4 message
    """
    print("--------------------------------------------------")
    print("--------------------------------------------------")
    print("--------------------------------------------------")
    TEMP_DATA_DUMP = S3_ROOT + "activity-stream/temp-testing-dir.parquet"
    print("using {} as temporary directory".format(TEMP_DATA_DUMP))
    print("checking {} dates".format(str(dates)))
    for date in dates:
        if type(date) != str:
            date = date_to_string(date, format='%Y-%m-%d')
        q = sql_query.format(START_DT=date,
                             END_DT=date)
        print("\n\n\n")
        print("--------------------------------------------------")
        print(date)
        print("--------------------------------------------------")
        print("checking:")
        print(q)
        try:
            test_data = pull_tiles_data(q, dbutils)
            test_count = test_data.count()

            write_parquet_to_s3(test_data, TEMP_DATA_DUMP)
            confirm_data = read_parquet_from_s3(TEMP_DATA_DUMP)

            confirm_count = confirm_data.count()
            if test_count == confirm_count:
                print("{}: data pulled and counts confirmed".format(date))
            elif test_count != confirm_count:
                print("{}: counts don't match").format(date)
        except Exception as E:
            print("{}: has error".format(date))
            print(E)
