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
    """
    def __init__(self, experiment_slug, start_date, num_days_enrollment=None):
        self.experiment_slug = experiment_slug
        self.start_date = start_date
        self.num_days_enrollment = num_days_enrollment

    def get_enrollments(self, spark):
        """Return a DataFrame of enrolled clients.

        This works for pref-flip studies.

        As of 2019/04/02, branch information isn't reliably available in
        the `events` table for addon experiments: branch may be NULL for
        all enrollments. The enrollment information for them is most
        reliably available in `telemetry_shield_study_parquet`. So use
        `get_enrollments_addon_exp()` for addon experiments until the
        underlying issue is resolved.
        Ref: https://bugzilla.mozilla.org/show_bug.cgi?id=1536644
        """
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
        # TODO: should we filter clients enrolled multiple times?
        return enrollments.select(
            enrollments.client_id,
            enrollments.submission_date_s3.alias('enrollment_date'),
            enrollments.event_map_values.branch.alias('branch'),
        )

    def get_enrollments_addon_exp(self, spark):
        """Temporary alternative to `get_enrollments` for addon experiments.

        As of 2019/04/02, branch information isn't reliably available in
        the `events` table for addon experiments: branch may be NULL for
        all enrollments. The enrollment information for them is most
        reliably available in `telemetry_shield_study_parquet`.
        Ref: https://bugzilla.mozilla.org/show_bug.cgi?id=1536644
        """
        tssp = spark.table('telemetry_shield_study_parquet')
        enrollments = tssp.filter(
            tssp.submission >= self.start_date
        ).filter(
            tssp.payload.data.study_state == 'enter'
        ).filter(
            tssp.payload.study_name == self.experiment_slug
        )

        if self.num_days_enrollment is not None:
            enrollments = enrollments.filter(
                tssp.submission < add_days(
                    self.start_date, self.num_days_enrollment
                )
            )

        # TODO: should we also call `broadcast()`? Should we cache?
        # TODO: should we filter clients enrolled multiple times?
        return enrollments.select(
            enrollments.client_id,
            enrollments.submission.alias('enrollment_date'),
            enrollments.payload.branch.alias('branch'),
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
            _Don't_ use `experiments`; as of 2019/04/02 it drops data
            collected after people self-unenroll, so unenrolling users
            will appear to churn.
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
        self._check_windows(today, conv_window_start_days + conv_window_length_days)

        # TODO: can/should we switch from submission_date_s3 to when the
        # events actually happened?
        res = enrollments.filter(
            # Ignore clients that might convert in the future
            # TODO: print debug info if it throws out enrollments
            # when `num_days_enrollment is not None`?
            enrollments.enrollment_date < add_days(
                today, -1 - conv_window_length_days - conv_window_start_days
            )
        ).join(
            self._filter_df_for_conv_window(
                df, today, conv_window_start_days, conv_window_length_days
            ),
            [
                # TODO: would it be faster if we enforce a join on sample_id?
                enrollments.client_id == df.client_id,

                # TODO: once we can rely on
                #   `df.experiments[self.experiment_slug]`
                # existing even after unenrollment, we could start joining on
                # branch to reduce problems associated with split client_ids.

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
        """Check that the conversion window dates make sense

        We need min_days_per_user days of post-enrollment data per user.
        This places limits on how early we can run certain analyses.
        This method calculates and presents these limits.
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

    def check_consistency(self, enrollments, df):
        """Check that the enrollments view is consistent with the data view

        i.e.:
        - no missing client_ids from either
        - check for duplicate client_id values
        """
        # TODO: stub
        raise NotImplementedError
