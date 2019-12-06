# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# This is a hack job to query an experiment in a mozanalysis-like way
# but with bigquery

import attr
import datetime
import re

from google.cloud import bigquery
from google.api_core.exceptions import Conflict


def add_days(date_string, n_days):
    """Add `n_days` days to a date string like '2019-01-01'."""
    original_date = datetime.datetime.strptime(date_string, '%Y-%m-%d')
    new_date = original_date + datetime.timedelta(days=n_days)
    return datetime.datetime.strftime(new_date, '%Y-%m-%d')


def date_sub(date_string_l, date_string_r):
    """Return the number of days between two date strings like '2019-01-01'"""
    date_l = datetime.datetime.strptime(date_string_l, '%Y-%m-%d')
    date_r = datetime.datetime.strptime(date_string_r, '%Y-%m-%d')
    return (date_l - date_r).days


def sanitize_table_name_for_bq(table_name):
    of_good_character_but_possibly_verbose = re.sub(r'[^a-zA-Z_0-9]', '_', table_name)

    if len(of_good_character_but_possibly_verbose) <= 1024:
        return of_good_character_but_possibly_verbose

    return of_good_character_but_possibly_verbose[:500] + '___' \
        + of_good_character_but_possibly_verbose[-500:]


class BigqueryStuff(object):
    def __init__(self, dataset_id, project_id='moz-fx-data-bq-data-science'):
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)


def run_query(bq_stuff, sql, results_table=None):
    """Run a query and return the result.

    If ``results_table`` is provided, then save the results
    into there (or just query from there if it already exists).
    """
    if not results_table:
        return bq_stuff.client.query(sql).result()

    try:
        full_res = bq_stuff.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                destination=bq_stuff.client.dataset(
                    bq_stuff.dataset_id
                ).table(results_table)
            )
        ).result()
        print('Saved into', results_table)
        return full_res

    except Conflict:
        print("Full results table already exists. Reusing", results_table)
        return bq_stuff.client.query(
            "SELECT * FROM `{project_id}.{dataset_id}.{full_table_name}`".format(
                project_id=bq_stuff.project_id,
                dataset_id=bq_stuff.dataset_id,
                full_table_name=results_table,
            )
        ).result()


@attr.s(frozen=True, slots=True)
class Experiment(object):

    experiment_slug = attr.ib()
    start_date = attr.ib()
    num_dates_enrollment = attr.ib(default=None)
    addon_version = attr.ib(default=None)

    def get_time_series_data(
        self, bq_stuff, metric_list, last_date_full_data,
        time_series_period='weekly', enrollments_query_type='normandy',
        custom_enrollments_query=None
    ):
        time_limits = TimeLimits.for_ts(
            self.start_date, last_date_full_data, time_series_period,
            self.num_dates_enrollment
        )

        full_sql = self._build_query(
            metric_list, time_limits, enrollments_query_type, custom_enrollments_query
        )

        full_res_table_name = sanitize_table_name_for_bq('_'.join(
            [last_date_full_data, self.experiment_slug, str(hash(full_sql))]
        ))

        full_res = run_query(bq_stuff, full_sql, full_res_table_name)

        ts_res = {

            aw.start: run_query(
                bq_stuff,
                self._build_analysis_window_subset_query(
                    bq_stuff, aw, full_res_table_name
                ),
            )

            for aw in time_limits.analysis_windows
        }

        return ts_res, full_res

    def get_single_window_data(
        self, bq_stuff, metric_list, last_date_full_data,
        analysis_start_days, analysis_length_days, enrollments_query_type='normandy',
        custom_enrollments_query=None
    ):
        time_limits = TimeLimits.for_single_analysis_window(
            self.start_date, last_date_full_data, analysis_start_days,
            analysis_length_days, self.num_dates_enrollment
        )

        full_sql = self._build_query(
            metric_list, time_limits, enrollments_query_type, custom_enrollments_query
        )

        full_res_table_name = sanitize_table_name_for_bq('_'.join(
            [last_date_full_data, self.experiment_slug, str(hash(full_sql))]
        ))

        return run_query(bq_stuff, full_sql, full_res_table_name)

    @staticmethod
    def _build_analysis_window_subset_query(
        bq_stuff, analysis_window, full_res_table_name
    ):
        return """
            SELECT * EXCEPT (client_id, analysis_window_start, analysis_window_end)
            FROM `{project_id}.{dataset_id}.{full_table_name}`
            WHERE analysis_window_start = {aws}
            AND analysis_window_end = {awe}
        """.format(
            project_id=bq_stuff.project_id,
            dataset_id=bq_stuff.dataset_id,
            full_table_name=full_res_table_name,
            aws=analysis_window.start,
            awe=analysis_window.end,
        )

    def _build_query(
        self, metric_list, time_limits, enrollments_query_type,
        custom_enrollments_query=None,
    ):
        analysis_windows_query = self._build_analysis_windows_query(
            time_limits.analysis_windows
        )

        enrollments_query = custom_enrollments_query or \
            self._build_enrollments_query(time_limits, enrollments_query_type)

        metrics_columns, metrics_queries = self._build_metrics_query_bits(
            metric_list, time_limits
        )

        return """
    WITH analysis_windows AS (
        {analysis_windows_query}
    ),
    raw_enrollments AS ({enrollments_query}),
    enrollments AS (
        SELECT
            e.*,
            aw.*
        FROM raw_enrollments e
        CROSS JOIN analysis_windows aw
    )
    SELECT
        enrollments.*,
        {metrics_columns}
    FROM enrollments
    {metrics_queries}
        """.format(
            analysis_windows_query=analysis_windows_query,
            enrollments_query=enrollments_query,
            metrics_columns=',\n        '.join(metrics_columns),
            metrics_queries='\n'.join(metrics_queries)
        )

    @staticmethod
    def _build_analysis_windows_query(analysis_windows):
        return "\n        UNION ALL\n        ".join(
            "(SELECT {aws} AS analysis_window_start, {awe} AS analysis_window_end)"
            .format(
                aws=aw.start,
                awe=aw.end,
            )
            for aw in analysis_windows
        )

    def _build_enrollments_query(self, time_limits, enrollments_query_type):
        if enrollments_query_type == 'normandy':
            return self._build_enrollments_query_normandy(time_limits)
        elif enrollments_query_type == 'glean':
            raise NotImplementedError
        else:
            raise ValueError

    def _build_enrollments_query_normandy(self, time_limits):
        return """
        SELECT
            e.client_id,
            `moz-fx-data-shared-prod.udf.get_key`(e.event_map_values, 'branch')
                AS branch,
            min(e.submission_date) AS enrollment_date,
            count(e.submission_date) AS num_enrollment_events
        FROM
            `moz-fx-data-shared-prod.telemetry.events` e
        WHERE
            e.event_category = 'normandy'
            AND e.event_method = 'enroll'
            AND e.submission_date
                BETWEEN '{first_enrollment_date}' AND '{last_enrollment_date}'
            AND e.event_string_value = '{experiment_slug}'
        GROUP BY e.client_id, branch
            """.format(
            experiment_slug=self.experiment_slug,
            first_enrollment_date=time_limits.first_enrollment_date,
            last_enrollment_date=time_limits.last_enrollment_date,
        )

    def _build_metrics_query_bits(self, metric_list, time_limits):
        bla = self._partition_metrics_by_data_source(metric_list)

        metrics_columns = []
        metrics_queries = []

        for i, ds in enumerate(bla.keys()):
            for m in bla[ds]:
                metrics_columns.append("ds_{i}.{metric_name}".format(
                    i=i, metric_name=m.name
                ))

            metrics_queries.append(
                """    LEFT JOIN (
        {query}
        ) ds_{i} USING (client_id, analysis_window_start, analysis_window_end)
                """.format(
                    query=ds.build_query(bla[ds], time_limits, self.experiment_slug),
                    i=i
                )
            )

        return metrics_columns, metrics_queries

    def _partition_metrics_by_data_source(self, metric_list):
        res = {}

        for m in reversed(metric_list):
            ds = m.data_source

            if ds not in res:
                res[ds] = m.data_source.get_sanity_metrics(self.experiment_slug)

            res[ds].insert(0, m)

        return res


@attr.s(frozen=True, slots=True)
class DataSource(object):
    name = attr.ib(validator=attr.validators.instance_of(str))
    from_expr = attr.ib(validator=attr.validators.instance_of(str))
    experiments_column_type = attr.ib(default='desktop_main_ping', type=str)
    client_id_column = attr.ib(default='client_id', type=str)
    submission_date_column = attr.ib(default='submission_date', type=str)

    @property
    def experiments_column_expr(self):
        if self.experiments_column_type is None:
            return ''

        elif self.experiments_column_type == 'desktop_main_ping':
            return """AND (
                    ds.{submission_date} != e.enrollment_date
                    OR `moz-fx-data-shared-prod.udf.get_key`(
                        ds.experiments, '{experiment_slug}'
                    ) IS NOT NULL
                )"""

        elif self.experiments_column_type == 'glean':
            raise NotImplementedError

        else:
            raise ValueError

    def build_query(self, metric_list, time_limits, experiment_slug):
        return """SELECT
            e.client_id,
            e.analysis_window_start,
            e.analysis_window_end,
            {metrics}
        FROM enrollments e
            LEFT JOIN {from_expr} ds
                ON ds.{client_id} = e.client_id
                AND ds.{submission_date} BETWEEN '{fddr}' AND '{lddr}'
                AND ds.{submission_date} BETWEEN
                    DATE_ADD(e.enrollment_date, interval e.analysis_window_start day)
                    AND DATE_ADD(e.enrollment_date, interval e.analysis_window_end day)
                {ignore_pre_enroll_first_day}
        GROUP BY e.client_id, e.analysis_window_start, e.analysis_window_end""".format(
            client_id=self.client_id_column,
            submission_date=self.submission_date_column,
            from_expr=self.from_expr,
            fddr=time_limits.first_date_data_required,
            lddr=time_limits.last_date_data_required,
            metrics=',\n            '.join(
                "{se} AS {n}".format(se=m.select_expr, n=m.name)
                for m in metric_list
            ),
            ignore_pre_enroll_first_day=self.experiments_column_expr.format(
                submission_date=self.submission_date_column,
                experiment_slug=experiment_slug,
            )
        )

    def get_sanity_metrics(self, experiment_slug):
        if self.experiments_column_type is None:
            return []

        elif self.experiments_column_type == 'desktop_main_ping':
            return [
                Metric(
                    name=self.name + '_has_contradictory_branch',
                    data_source=self,
                    select_expr=agg_any("""`moz-fx-data-shared-prod.udf.get_key`(
                ds.experiments, '{experiment_slug}'
            ) != e.branch""".format(experiment_slug=experiment_slug)),
                ),
                Metric(
                    name=self.name + '_has_non_enrolled_data',
                    data_source=self,
                    select_expr=agg_any("""`moz-fx-data-shared-prod.udf.get_key`(
                ds.experiments, '{experiment_slug}'
            ) IS NULL""".format(experiment_slug=experiment_slug))
                ),
            ]

        elif self.experiments_column_type == 'glean':
            raise NotImplementedError

        else:
            raise ValueError


@attr.s(frozen=True, slots=True)
class Metric(object):
    name = attr.ib(type=str)
    data_source = attr.ib(type=DataSource)
    select_expr = attr.ib(type=str)


clients_daily = DataSource(
    name='clients_daily',
    from_expr="`moz-fx-data-shared-prod.telemetry.clients_daily`",
)

search_clients_daily = DataSource(
    name='search_clients_daily',
    from_expr='`moz-fx-data-shared-prod.search.search_clients_daily`',
    experiments_column_type=None,
)


def agg_sum(select_expr):
    return "COALESCE(SUM({}), 0)".format(select_expr)


def agg_any(select_expr):
    return "COALESCE(CAST(MAX({}) AS INT64), 0)".format(select_expr)


active_hours = Metric(
    name='active_hours',
    data_source=clients_daily,
    select_expr=agg_sum('active_hours_sum')
)

uri_count = Metric(
    name='uri_count',
    data_source=clients_daily,
    select_expr=agg_sum('scalar_parent_browser_engagement_total_uri_count_sum')
)

search_count = Metric(
    name='search_count',
    data_source=search_clients_daily,
    select_expr=agg_sum('sap')
)

ad_clicks = Metric(
    name='ad_clicks',
    data_source=search_clients_daily,
    select_expr=agg_sum('ad_click')
)


@attr.s(frozen=True, slots=True)
class TimeLimits(object):
    """Internal object containing various time limits.

    Instantiated and used by the ``Experiment`` class; end users
    should not need to interact with it.

    Do not directly instantiate: use the constructors provided.

    There are several time constraints needed to specify a valid query
    for experiment data:

        * When did enrollments start?
        * When did enrollments stop?
        * How long after enrollment does the analysis window start?
        * How long is the analysis window?

    Even if these four quantities are specified directly, it is
    important to check that they are consistent with the available
    data - i.e. that we have data for the entire analysis window for
    every enrollment.

    Furthermore, there are some extra quantities that are useful for
    writing efficient queries:

        * What is the first date for which we need data from our data
          source?
        * What is the last date for which we need data from our data
          source?

    Instances of this class store all these quantities and do validation
    to make sure that they're consistent. The "store lots of overlapping
    state and validate" strategy was chosen over "store minimal state
    and compute on the fly" because different state is supplied in
    different contexts.
    """

    first_enrollment_date = attr.ib(type=str)
    last_enrollment_date = attr.ib(type=str)

    first_date_data_required = attr.ib(type=str)
    last_date_data_required = attr.ib(type=str)

    analysis_windows = attr.ib()  # type: tuple[AnalysisWindow]

    @classmethod
    def for_single_analysis_window(
        cls,
        first_enrollment_date,
        last_date_full_data,
        analysis_start_days,
        analysis_length_dates,
        num_dates_enrollment=None,
    ):
        """Return a ``TimeLimits`` instance with the following parameters

        Args:
            first_enrollment_date (str): First date on which enrollment
                events were received; the start date of the experiment.
            last_date_full_data (str): The most recent date for which we
                have complete data, e.g. '20190322'. If you want to ignore
                all data collected after a certain date (e.g. when the
                experiment recipe was deactivated), then do that here.
            analysis_start_days (int): the start of the analysis window,
                measured in 'days since the client enrolled'. We ignore data
                collected outside this analysis window.
            analysis_length_days (int): the length of the analysis window,
                measured in days.
            num_dates_enrollment (int, optional): Only include this many days
                of enrollments. If ``None`` then use the maximum number of days
                as determined by the metric's analysis window and
                ``last_date_full_data``. Typically ``7n+1``, e.g. ``8``. The
                factor ``7`` removes weekly seasonality, and the ``+1``
                accounts for the fact that enrollment typically starts a few
                hours before UTC midnight.
        """
        analysis_window = AnalysisWindow(
            analysis_start_days, analysis_start_days + analysis_length_dates - 1
        )

        if num_dates_enrollment is None:
            last_enrollment_date = add_days(last_date_full_data, -analysis_window.end)

        else:
            last_enrollment_date = add_days(
                first_enrollment_date, num_dates_enrollment - 1
            )

            if add_days(
                last_enrollment_date, analysis_window.end
            ) > last_date_full_data:
                raise ValueError(
                    "You said you wanted {} dates of enrollment, ".format(
                        num_dates_enrollment
                    ) + "and need data from the {}th day after enrollment. ".format(
                        analysis_window.end
                    ) + "For that, you need to wait until we have data for {}.".format(
                        last_enrollment_date
                    )
                )

        first_date_data_required = add_days(
            first_enrollment_date, analysis_window.start
        )
        last_date_data_required = add_days(last_enrollment_date, analysis_window.end)

        tl = cls(
            first_enrollment_date=first_enrollment_date,
            last_enrollment_date=last_enrollment_date,
            first_date_data_required=first_date_data_required,
            last_date_data_required=last_date_data_required,
            analysis_windows=(analysis_window,),
        )
        return tl

    @classmethod
    def for_ts(
        cls,
        first_enrollment_date,
        last_date_full_data,
        time_series_period,
        num_dates_enrollment,
    ):
        """Return a ``TimeLimits`` instance for a time series.

        Args:
            first_enrollment_date (str): First date on which enrollment
                events were received; the start date of the experiment.
            last_date_full_data (str): The most recent date for which we
                have complete data, e.g. '20190322'. If you want to ignore
                all data collected after a certain date (e.g. when the
                experiment recipe was deactivated), then do that here.
            time_series_period: 'daily' or 'weekly'.
            num_dates_enrollment (int): Take this many days of client
                enrollments. This is a mandatory argument because it
                determines the number of points in the time series.
        """
        if time_series_period not in ('daily', 'weekly'):
            raise ValueError("Unsupported time series period {}".format(
                time_series_period
            ))

        analysis_window_length_dates = 1 if time_series_period == 'daily' else 7

        last_enrollment_date = add_days(
            first_enrollment_date, num_dates_enrollment - 1
        )
        max_dates_of_data = date_sub(last_date_full_data, last_enrollment_date) + 1
        num_periods = max_dates_of_data // analysis_window_length_dates

        if num_periods <= 0:
            raise ValueError("Insufficient data")

        analysis_windows = tuple([
            AnalysisWindow(
                i * analysis_window_length_dates,
                (i + 1) * analysis_window_length_dates - 1
            )
            for i in range(num_periods)
        ])

        last_date_data_required = add_days(
            last_enrollment_date, analysis_windows[-1].end
        )

        return cls(
            first_enrollment_date=first_enrollment_date,
            last_enrollment_date=last_enrollment_date,
            first_date_data_required=first_enrollment_date,
            last_date_data_required=last_date_data_required,
            analysis_windows=analysis_windows,
        )

    @first_enrollment_date.validator
    def _validate_first_enrollment_date(self, attribute, value):
        assert self.first_enrollment_date <= self.last_enrollment_date
        assert self.first_enrollment_date <= self.first_date_data_required
        assert self.first_enrollment_date <= self.last_date_data_required

    @last_enrollment_date.validator
    def _validate_last_enrollment_date(self, attribute, value):
        assert self.last_enrollment_date <= self.last_date_data_required

    @first_date_data_required.validator
    def _validate_first_date_data_required(self, attribute, value):
        assert self.first_date_data_required <= self.last_date_data_required

        min_analysis_window_start = min(aw.start for aw in self.analysis_windows)
        assert self.first_date_data_required == add_days(
            self.first_enrollment_date, min_analysis_window_start
        )

    @last_date_data_required.validator
    def _validate_last_date_data_required(self, attribute, value):
        max_analysis_window_end = max(aw.end for aw in self.analysis_windows)
        assert self.last_date_data_required == add_days(
            self.last_enrollment_date, max_analysis_window_end
        )


@attr.s(frozen=True, slots=True)
class AnalysisWindow(object):
    """Represents the range of days in which to measure a metric.

    The range is measured in "days after enrollment", and is inclusive.

    For example, ``AnalysisWindow(0, 6)`` is the first week after enrollment.

    Args:
        start (int): First day of the analysis window, in days since
            enrollment.
        end (int): Final day of the analysis window, in days since
            enrollment.
    """
    start = attr.ib(type=int)
    end = attr.ib(type=int)

    @start.validator
    def _validate_start(self, attribute, value):
        assert value >= 0

    @end.validator
    def _validate_end(self, attribute, value):
        assert value >= self.start
