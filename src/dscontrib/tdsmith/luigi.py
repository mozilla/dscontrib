"""
Luigi!

You may find yourself in the following scenario:
* You have a bunch of .sql files, which may or may not depend on each other,
  that define some queries. You're probably interested in materializing some,
  but not necessarily all, of those queries directly into .csv.gz files in your
  working directory.
* You'd like to update your local .csv files update when you change one of your .sql
  files.
* You don't want to run more queries than necessary.
* You don't want to have to copy-paste things and hold the state of your query
  DAG in your head.

This is for you! This helps you use Luigi to manage the state of your workflow.
It works great for either simple or complex cases.

We don't need to worry too much about how Luigi works in order to use this,
but it's good to know that Luigi has a concept of _tasks_,
which have dependencies, take inputs, and produce outputs.

This file defines some different kinds of tasks. You declare your specific
workflow by inheriting from them.

An example workflow looks like:

### workflow.py

import luigi
from dscontrib.tdsmith.luigi import *

class ExtractPingsTable(TableFromStalableSql):
    # Run a SQL job and save the results to a BQ table.
    # This doesn't download any rows directly.
    # It's great for subsetting down to pings you need
    # so that they're faster to query later.
    query_filename = "extract_pings.sql"
    destination_table = "moz-fx-data-bq-data-science.tdsmith.extracted_pings"

class SummarizePingsTable(TableFromStalableSql):
    # Same thing, except this job depends on the extract job.
    query_filename = "summarize_pings.sql"
    destination_table = "moz-fx-data-bq-data-science.tdsmith.summarized_pings"

    # This is how we express that this job depends on the last one:
    def requires(self):
        return ExtractPingsTable()

class PingSummaryCsv(ExtractedCsvFromStalableTable):
    # This class keeps a local CSV file up to date with a BigQuery table.
    source_table = SummarizePings.destination_table
    output_filename = "summary.csv.gz"

    def requires(self):
        return SummarizePingsTable()

# We can also define other queries that write directly to a .csv.
# This is useful for small result sets.
class EnrollmentCsv(CsvFromStalableSql):
    query_filename = "enrollment.sql"
    output_filename = "enrollment.csv.gz"

# Finally, we can define a "parent" task that makes sure everything gets run.
# It should depend on all of your CSVs.
class MyWorkflow(luigi.task):
    def requires(self):
        return [
            PingSummaryCsv(),
            EnrollmentCsv(),
        ]
###

Now, you can run this with:
    PYTHONPATH=$PWD python -m luigi --module workflow MyWorkflow --local-scheduler

The first time, it will run all your queries.
The second time, it won't run any of them, until you change the .sql files!
"""


import datetime as dt
from functools import partial
import json
from pathlib import Path
import logging

import attr
from google.cloud import bigquery as bq
from google.cloud import storage as storage
from google.api_core import exceptions as bq_ex
import luigi
from pytz import utc


__all__ = [
    "CsvFromStalableSql",
    "TableFromStalableSql",
    "ExtractedCsvFromStalableTable",
]


PROJECT = "moz-fx-data-bq-data-science"
logger = logging.getLogger("luigi-interface")


@attr.s
class BigQueryTarget(luigi.Target):
    table: bq.table.TableReference = attr.ib()
    client: bq.Client = attr.ib(default=attr.Factory(partial(bq.Client, PROJECT)))

    _table_object: bq.table.Table = attr.ib(default=None)

    @property
    def table_object(self):
        if self._table_object:
            return self._table_object
        try:
            self._table_object = self.client.get_table(self.table)
        except bq_ex.NotFound:
            self._table_object = None
        return self._table_object

    def exists(self):
        return self.table_object is not None


class CsvFromStalableSql(luigi.Task):
    """Return a small result set from an anonymous query."""

    @property
    def query_filename(self):
        raise NotImplementedError

    @property
    def output_filename(self):
        raise NotImplementedError

    query_parameters = None

    __client = None

    @property
    def client(self):
        self.__client = self.__client or bq.Client(PROJECT)
        return self.__client

    def run(self):
        query = Path(self.query_filename).read_text()

        query_parameters = []
        if self.query_parameters:
            query_parameters = [
                bq.query._query_param_from_api_repr(i)
                for i in json.loads(Path(self.query_parameters).read_text())
            ]

        job = self.client.query(
            query,
            bq.job.QueryJobConfig(
                query_parameters=query_parameters,
                use_legacy_sql=False,
            ),
        )
        df = job.to_dataframe()
        df.to_csv(self.output_filename, index=False)

    def output(self):
        return luigi.LocalTarget(self.output_filename)

    def complete(self):
        for task in luigi.task.flatten(self.requires()):
            if not task.complete():
                return False

        output_path = Path(self.output().path)
        if not output_path.exists():
            return False
        output_mtime = output_path.stat().st_mtime
        input_mtime = Path(self.query_filename).stat().st_mtime
        return input_mtime < output_mtime


class TableFromStalableSql(luigi.Task):
    @property
    def query_filename(self):
        raise NotImplementedError

    @property
    def destination_table(self):
        raise NotImplementedError

    query_parameters = None

    __client = None

    @property
    def client(self):
        self.__client = self.__client or bq.Client(PROJECT)
        return self.__client

    def run(self):
        logger.info("Running query")
        query = Path(self.query_filename).read_text()

        query_parameters = []
        if self.query_parameters:
            query_parameters = [
                bq.query._query_param_from_api_repr(i)
                for i in json.loads(Path(self.query_parameters).read_text())
            ]

        job = self.client.query(
                query,
                bq.job.QueryJobConfig(
                    destination=self.destination_table,
                    query_parameters=query_parameters,
                    use_legacy_sql=False,
                    write_disposition=bq.job.WriteDisposition.WRITE_TRUNCATE,
                )
        )
        job.result(max_results=1)

    def output(self):
        ref = bq.table.TableReference.from_string(self.destination_table)
        return BigQueryTarget(ref, self.client)

    def complete(self):
        for task in luigi.task.flatten(self.requires()):
            if not task.complete():
                return False

        table = self.output().table_object
        if table is None:
            logger.info("Destination table does not exist")
            return False

        query_mtime = Path(self.query_filename).stat().st_mtime
        query_mtime_dt = dt.datetime.fromtimestamp(query_mtime, utc)
        logger.info(f"Local: {query_mtime_dt} Remote: {table.modified}")
        return query_mtime_dt < table.modified


class ExtractedCsvFromStalableTable(luigi.Task):
    @property
    def source_table(self):
        raise NotImplementedError

    @property
    def output_filename(self):
        raise NotImplementedError

    @property
    def storage_url(self):
        """gs://bucket/filename.csv.gz"""
        raise NotImplementedError

    __client = None

    @property
    def client(self):
        self.__client = self.__client or bq.Client(PROJECT)
        return self.__client

    def run(self):
        job = self.client.extract_table(
            self.source_table,
            self.storage_url,
            job_config=bq.job.ExtractJobConfig(
                compression=bq.job.Compression.GZIP,
                destination_format=bq.job.DestinationFormat.CSV,
            )
        )
        job.result()

        storage_client = storage.client.Client(self.client.project)

        with open(self.output_filename, "wb") as f:
            storage_client.download_blob_to_file(self.storage_url, f)

    def output(self):
        return luigi.LocalTarget(self.output_filename)

    def complete(self):
        for task in luigi.task.flatten(self.requires()):
            if not task.complete():
                return False

        output = Path(self.output_filename)
        if not output.exists():
            return False
        output_mtime = dt.datetime.fromtimestamp(output.stat().st_mtime, utc)

        source_table_obj = self.client.get_table(self.source_table)
        return source_table_obj.modified < output_mtime
