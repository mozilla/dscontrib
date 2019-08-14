# activity stream utils

# pull_tiles_data(sql_query, pw)


# activity stream utils: imports
from pyspark.sql import SparkSession
import socket

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
        jdbcurl = ("jdbc:postgresql://{0}:{1}/tiles?user={2}" +
                   "&password={3}&ssl=true&sslMode=verify-ca")\
                   .format(hostname, port,
                           dbutils.secrets.get('tiles-redshift', 'username'),
                           dbutils.secrets.get('tiles-redshift', 'password')
                           ))
        return jdbcurl

    TEMPDIR = "s3n://mozilla-databricks-telemetry-test/tiles-redshift/_temp"
    JDBC_URL = tiles_redshift_jdbcurl_fetch()

    df = spark.read.format("com.databricks.spark.redshift")\
                   .option("forward_spark_s3_credentials", True)\
                   .option("url", JDBC_URL).option("tempdir", TEMPDIR)\
                   .option("query", sql_query)\
                   .load()
    return df
