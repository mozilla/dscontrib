# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from pyspark.sql import functions as F

from dscontrib.flawrence.util import add_days


class Experiment(object):
    """Get DataFrames of experiment data; store experiment metadata.

    FIXME: choose a smaller study so it's faster to run the example
    Example usage:
        study_name = 'pref-flip-defaultoncookierestrictions-1523780'  # new
        ms = spark.table('main_summary')

        experiment = Experiment(study_name, start_date='20190210')
        enrollments = experiment.get_enrollments(spark)

        res = experiment.get_per_client_data(
            enrollments,
            ms,
            [
                F.sum(F.coalesce(
                    ms.scalar_parent_browser_search_ad_clicks.google, F.lit(0)
                )).alias('ad_clicks'),
                F.sum(F.coalesce(
                    ms.scalar_parent_browser_search_with_ads.google, F.lit(0)
                )).alias('search_with_ads'),
            ],
            today='20190325',
            conv_window_start_days=0,
            conv_window_length_days=7
        )

    Args:
        experiment_slug (str): Name of the study, used to identify
            the enrollment events specific to this study.
        start_date (str): e.g. '20190101'. First date on which enrollment
            events were received.
        num_days_enrollment (int, optional): Only include this many days
            of enrollments. If `None` then use the maximum number of days
            as determined by the metric's conversion window and today's date.

    Attributes:
        experiment_slug (str): Name of the study, used to identify
            the enrollment events specific to this study.
        start_date (str): e.g. '20190101'. First date on which enrollment
            events were received.
        num_days_enrollment (int, optional): Only include this many days
            of enrollments. If `None` then use the maximum number of days
            as determined by the metric's conversion window and today's date.
    """
    def __init__(self, experiment_slug, start_date, num_days_enrollment=None):
        self.experiment_slug = experiment_slug
        self.start_date = start_date
        self.num_days_enrollment = num_days_enrollment

    def get_enrollments(self, spark, study_type='pref_flip'):
        """Return a DataFrame of enrolled clients.

        This works for pref-flip and addon studies.

        The underlying queries are different for pref-flip vs addon
        studies, because as of 2019/04/02, branch information isn't
        reliably available in the `events` table for addon experiments:
        branch may be NULL for all enrollments. The enrollment
        information for them is most reliably available in
        `telemetry_shield_study_parquet`. Once this issue is resolved,
        we will probably start using normandy events for all desktop
        studies.
        Ref: https://bugzilla.mozilla.org/show_bug.cgi?id=1536644

        Args:
            spark: The spark context.
            study_type (str): One of the following strings:
                - 'pref_flip'
                - 'addon'

        Returns:
            A Spark DataFrame of enrollment data. One row per
            enrollment. Columns:
                - client_id (str)
                - enrollment_date (str): e.g. '20190329'
                - branch (str)
        """
        if study_type == 'pref_flip':
            enrollments = self._get_enrollments_normandy_events(spark)

        elif study_type == 'addon':
            enrollments = self._get_enrollments_addon_exp(spark)

        elif study_type == 'glean':
            raise NotImplementedError

        else:
            raise ValueError("Unrecognized study_type: {}".format(study_type))

        enrollments = enrollments.filter(
            enrollments.enrollment_date >= self.start_date
        )

        if self.num_days_enrollment is not None:
            enrollments = enrollments.filter(
                enrollments.enrollment_date < add_days(
                    self.start_date, self.num_days_enrollment
                )
            )

        enrollments.cache()
        F.broadcast(enrollments)

        return enrollments

    def _get_enrollments_normandy_events(self, spark):
        """Return a DataFrame of enrolled clients for a pref-flip study.

        Args:
            spark: The spark context.
        """
        events = spark.table('events')

        return events.filter(
            events.event_category == 'normandy'
        ).filter(
            events.event_method == 'enroll'
        ).filter(
            events.event_string_value == self.experiment_slug
        ).select(
            events.client_id,
            events.submission_date_s3.alias('enrollment_date'),
            events.event_map_values.branch.alias('branch'),
        )

    def _get_enrollments_addon_exp(self, spark):
        """Return a DataFrame of enrolled clients for an addon study.

        Args:
            spark: The spark context.
        """
        tssp = spark.table('telemetry_shield_study_parquet')

        return tssp.filter(
            tssp.payload.data.study_state == 'enter'
        ).filter(
            tssp.payload.study_name == self.experiment_slug
        ).select(
            tssp.client_id,
            tssp.submission.alias('enrollment_date'),
            tssp.payload.branch.alias('branch'),
        )

    def get_per_client_data(
        self, enrollments, df, metric_list, today, conv_window_start_days,
        conv_window_length_days, keep_client_id=False
    ):
        """Return a DataFrame containing per-client metric values.

        Args:
            enrollments: A spark DataFrame of enrollments, like the one
                returned by `self.get_enrollments()`.
            df: A spark DataFrame containing the data needed to calculate
                the metrics. Could be `main_summary` or `clients_daily`.
                _Don't_ use `experiments`; as of 2019/04/02 it drops data
                collected after people self-unenroll, so unenrolling users
                will appear to churn.
            metric_list: A list of columns that aggregate and compute
                metrics, e.g.
                `[F.coalesce(F.sum(df.metric_name), F.lit(0)).alias('metric_name')]`
            today (str): The most recent day for which we have incomplete
                data, e.g. '20190322'. Feel free to supply an older date
                if the ETL is delayed or the experiment ended in the past.
            conv_window_start_days (int): the start of the conversion window,
                measured in 'days since the user enrolled'. We ignore data
                collected outside this conversion window.
            conv_window_length_days (int): the length of the conversion window,
                measured in days.
            keep_client_id (bool): Whether to return a `client_id` column.
                Defaults to False to reduce memory usage of the results.

        Returns:
            A spark DataFrame of experiment data. One row per `client_id`.
            One or two metadata columns, then one column per metric in
            `metric_list`. Then one column per sanity-check metric.
            Columns:
                - client_id (str, optional): Not necessary for
                    "happy path" analyses.
                - branch (str): The client's branch
                - [metric 1]: The client's value for the first metric in
                    `metric_list`.
                - ...
                - [metric n]: The client's value for the nth (final)
                    metric in `metric_list`.
                - [sanity check 1]: The client's value for the first
                    sanity check metric.
                - ...
                - [sanity check n]: The client's value for the last
                    sanity check metric.

            This format - the schema plus there being one row per
            enrolled client, regardless of whether the client has data
            in `df` - was agreed upon by the DS team, and is the
            standard format for queried experimental data.
        """
        self._check_windows(today, conv_window_start_days + conv_window_length_days)

        # TODO accuracy: can/should we switch from submission_date_s3 to when the
        # events actually happened?
        res = enrollments.filter(
            # Ignore clients that might convert in the future
            enrollments.enrollment_date < add_days(
                today, -1 - conv_window_length_days - conv_window_start_days
            )
        ).join(
            self._filter_df_for_conv_window(
                df, today, conv_window_start_days, conv_window_length_days
            ),
            [
                # TODO perf: would it be faster if we enforce a join on sample_id?
                enrollments.client_id == df.client_id,

                # TODO accuracy: once we can rely on
                #   `df.experiments[self.experiment_slug]`
                # existing even after unenrollment, we could start joining on
                # branch to reduce problems associated with split client_ids.

                # Do a quick pass aiming to efficiently filter out lots of rows:
                enrollments.enrollment_date <= df.submission_date_s3,

                # Now do a more thorough pass filtering out irrelevant data:
                # TODO perf: what is a more efficient way to do this?
                (
                    (
                        F.unix_timestamp(df.submission_date_s3, 'yyyyMMdd')
                        - F.unix_timestamp(enrollments.enrollment_date, 'yyyyMMdd')
                    ) / (24 * 60 * 60)
                ).between(
                    conv_window_start_days,
                    conv_window_start_days + conv_window_length_days
                ),

                # Try to filter data from day of enrollment before time of enrollment.
                # If the client enrolled and unenrolled on the same day then this
                # will also filter out that day's post unenrollment data but that's
                # probably the smallest and most innocuous evil on the menu.
                (
                    (enrollments.enrollment_date != df.submission_date_s3)
                    | (~F.isnull(df.experiments[self.experiment_slug]))
                ),

            ],
            'left'
        ).groupBy(
            enrollments.client_id, enrollments.branch
        ).agg(
            *(metric_list + self._get_telemetry_sanity_check_metrics(enrollments, df))
        )
        if keep_client_id:
            return res
        else:
            return res.drop(enrollments.client_id)

    def _check_windows(self, today, min_days_per_user):
        """Check that the conversion window dates make sense.

        We need `min_days_per_user` days of post-enrollment data per user.
        This places limits on how early we can run certain analyses.
        This method calculates and presents these limits.

        Args:
            today (str): The most recent day for which we have incomplete
                data, e.g. '20190322'. Feel free to supply an older date
                if the ETL is delayed or the experiment ended in the past.
            min_days_per_user (int): The minimum number of days of
                post-enrollment data required to have data for the user
                for the entire conversion window.
        """
        slack = 1  # 1 day of slack: assume yesterday's data is not present
        last_enrollment_date = add_days(
            today, -1 - min_days_per_user - slack
        )

        if self.num_days_enrollment is not None:
            official_last_enrollment_date = add_days(
                self.start_date, self.num_days_enrollment - 1
            )
            assert last_enrollment_date >= official_last_enrollment_date, \
                "You said you wanted {} days of enrollment, ".format(
                    self.num_days_enrollment
                ) + "but your conversion window of {} days won't have ".format(
                    min_days_per_user
                ) + "complete data until we have the data for {}.".format(
                    add_days(official_last_enrollment_date, 1 + min_days_per_user)
                )

            last_enrollment_date = official_last_enrollment_date

        print("Taking enrollments between {} and {}".format(
            self.start_date, last_enrollment_date
        ))
        assert self.start_date <= last_enrollment_date, \
            "No users have had time to convert yet"

    def _filter_df_for_conv_window(
        self, df, today, conv_window_start_days, conv_window_length_days
    ):
        """Return the df, filtered to the relevant dates.

        This should not affect the results - it should just speed
        things up.
        """
        if self.num_days_enrollment is not None:
            # Ignore data after the conversion window of the last enrollment
            df = df.filter(
                df.submission_date_s3 <= add_days(
                    self.start_date,
                    self.num_days_enrollment + conv_window_start_days
                    + conv_window_length_days
                )
            )

        # Ignore data before the conversion window of the first enrollment
        return df.filter(df.submission_date_s3 >= add_days(
            self.start_date, conv_window_start_days
        ))

    def _get_telemetry_sanity_check_metrics(self, enrollments, df):
        """Return aggregations that check for problems with a client."""

        # TODO: Once we know what form the metrics library will take,
        # we should move the below metric definitions and documentation
        # into it.
        return [

            # Check to see whether the client_id is also enrolled in other branches
            # E.g. indicates cloned profiles. Fraction of such users should be
            # small, and similar between branches.
            F.max(F.coalesce((
                df.experiments[self.experiment_slug] != enrollments.branch
            ).astype('int'), F.lit(0))).alias('has_contradictory_branch'),

            # Check to see whether the client_id was sending data in the conversion
            # window that wasn't tagged as being part of the experiment. Indicates
            # either a client_id clash, or the client unenrolling. Fraction of such
            # users should be small, and similar between branches.
            F.max(F.coalesce((
                ~F.isnull(df.experiments)
                & F.isnull(df.experiments[self.experiment_slug])
            ).astype('int'), F.lit(0))).alias('has_non_enrolled_data'),
        ]
