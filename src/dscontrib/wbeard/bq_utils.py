from os.path import abspath, expanduser
import os
from pathlib import PosixPath
import re

import pandas as pd
from google.oauth2 import service_account  # type: ignore

# bucket: 'moz-fx-data-derived-datasets-analysis'
PROJ_DS = "moz-fx-data-bq-data-science"
PROJ_DD = "moz-fx-data-derived-datasets"
PROJ_SHARED = "moz-fx-data-shared-prod"

_loc = os.environ.get("BQ_CREDS")
if _loc is not None:
    creds_loc = abspath(expanduser(_loc))
    creds = service_account.Credentials.from_service_account_file(creds_loc)
else:
    creds = None
    print("Warning, $BQ_CREDS not found in environment")


def bq_read(q):
    return pd.read_gbq(
        q, project_id=creds.project_id, credentials=creds, dialect="standard"
    )


def try_read(fn):
    try:
        path = PosixPath(fn)
        if path.exists():
            with open(path, "r") as fp:
                q = fp.read()
    except OSError:
        q = fn
    return q


def bq_read2(q):
    q = try_read(q)
    return pd.read_gbq(
        q, project_id=creds.project_id, credentials=creds, dialect="standard"
    )


udf = """
CREATE TEMP FUNCTION nano_dt(x INT64) AS (
  DATETIME(TIMESTAMP_SECONDS(div(x, CAST(1e9 as int64)) ))
);
"""


def to_sql_column_name(s):
    s = s.lower()
    return re.sub(r"[^A-Za-z\d_]+", "_", s)
