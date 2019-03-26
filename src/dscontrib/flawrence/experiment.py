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

        experiment = Experiment(study_name, '20190210')
        enrollments = experiment.get_enrollments(spark)

        res = experiment.get_per_client_data(
            enrollments,
            experiment.filter_df(ms),
            [
                F.sum(F.coalesce(ms.scalar_parent_browser_search_ad_clicks.google, F.lit(0))).alias('ad_clicks'),
                F.sum(F.coalesce(ms.scalar_parent_browser_search_with_ads.google, F.lit(0))).alias('search_with_ads'),
            ],
            '20190325',
            0,
            7
        )
    """
    def __init__(self, experiment_slug, start_date, num_days_enrollment=None):
        self.experiment_slug = experiment_slug
        self.start_date = start_date
        self.num_days_enrollment = num_days_enrollment

    def filter_df(self, df):
        return df.filter(
            ~F.isnull(df.experiments[self.experiment_slug])
        ).filter(
            df.submission_date_s3 >= self.start_date
        ).withColumn(
            'branch', df.experiments[self.experiment_slug]
        )

    def get_enrollments(self, spark):
        events = spark.table('events')
        enrollments = events.filter(
            events.submission_date_s3 >= self.start_date
        ).filter(
            events.event_category == 'normandy'
        ).filter(
            events.event_method == 'enroll'
        ).filter(
            events.event_string_value == self.experiment_slug
        )

        if self.num_days_enrollment is not None:
            enrollments = enrollments.filter(
                events.submission_date_s3 < add_days(
                    self.start_date, self.num_days_enrollment
                )
            )

        # TODO: should we also call `broadcast()`? Should we cache?
        return enrollments.select(
            enrollments.client_id,
            enrollments.submission_date_s3.alias('enrollment_date'),
            enrollments.event_map_values.branch.alias('branch'),
        )

    def get_per_client_data(
        self, enrollments, df, metric_list, today, conv_window_start_days,
        conv_window_length_days, keep_client_id=False
    ):
        """Return a DataFrame containing per-client metric values.

        Args:
        - enrollments: A spark DataFrame of enrollments, like the one
            returned by `self.get_enrollments()`.
        - df: A spark DataFrame containing the data needed to calculate
            the metrics. Could be `main_summary` or `clients_daily`.
        - metric_list: A list of columns that aggregate and compute
            metrics, e.g.
                `[F.coalesce(F.sum(df.metric_name), F.lit(0)).alias('metric_name')]`
        - today: A string representing the most recent day for which we
            have incomplete data, e.g. '20190322'.
        - conv_window_start_days: the start of the conversion window,
            measured in 'days since the user enrolled'. We ignore data
            collected outside this conversion window.
        - conv_window_length_days: the length of the conversion window,
            measured in days.
        - keep_client_id: Whether to return a `client_id` column. Defaults
            to False to reduce memory usage of the results.
        """
        # TODO: can/should we switch from submission_date_s3 to when the
        # events actually happened?
        res = enrollments.filter(
            # Ignore clients that might convert in the future
            # TODO: print debug info if it throws out enrollments
            # when `num_days_enrollment is not None`?
            enrollments.enrollment_date < add_days(
                today,
                -1 - conv_window_length_days - conv_window_start_days
            )
        ).join(
            # TODO: coarsely filter `df` by conv window and enrollment dates
            df,
            [
                # TODO: would it be faster if we enforce a join on sample_id?
                enrollments.client_id == df.client_id,
                enrollments.branch == df.branch,
                # Do a quick pass aiming to efficiently filter out lots of rows:
                enrollments.enrollment_date <= df.submission_date_s3,
                # Now do a more thorough pass filtering out irrelevant data:
                # TODO: is there a more efficient way to do this?
                (
                    (
                        F.unix_timestamp(df.submission_date_s3, 'yyyyMMdd')
                        - F.unix_timestamp(enrollments.enrollment_date, 'yyyyMMdd')
                    ) / (24 * 60 * 60)
                ).between(
                    conv_window_start_days,
                    conv_window_start_days + conv_window_length_days
                )

            ],
            'left'
        ).groupBy(
            enrollments.client_id, enrollments.branch
        ).agg(
            *metric_list
        )
        if keep_client_id:
            return res
        else:
            return res.drop(enrollments.client_id)

    def check_consistency(self, enrollments, df):
        """Check that the enrollments view is consistent with the data view

        i.e.:
        - no missing client_ids from either
        - check for duplicate client_id values
        """
        # TODO: stub
        raise NotImplementedError
