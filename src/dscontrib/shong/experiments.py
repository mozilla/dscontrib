# experiment_membership_df(slug, date_start, enrollment_period, observation_period)
# ms_pings_subset_df(df, date_start, total_period, columns=MS_USAGE_COLS, slug=None)
# as_pings_subset_df(as_df, date_start, total_period, slug=None)
# experiment_pings(pings_df, membership_df, observation_period)

# spark imports
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import MapType, StringType

# local imports
from .constants import MS_USAGE_COLS
from .activitystream import as_experiment_field
from .util import date_plus_N, date_to_string, string_to_date


# fetch spark context
spark = SparkSession.builder.getOrCreate()


def experiment_membership_df(slug, date_start, enrollment_period, observation_period):
    """
    notes on this methodology:
    - enrollments and unenrollments: only distinct client_id, branch
      (multiple pings collapsed, first ping counted)
    - join unenrollments by client_id, branch
      (can have unenrollment in one branch and not in other)
    - unenrollments without branch are ignored
      (even if there is an enrollment ping with same client_id)

    table schema:
      client_id
      branch
      enrollment_dt
      ping_count_enrollment
      unenrollment_dt
      ping_count_unenrollment
    """

    # function can take datetime or the s3 date string
    if type(date_start) == str:
        date_start = string_to_date(date_start)

    # get dates: enrollment period and max obs period
    date_enroll_end = date_plus_N(date_start, enrollment_period)
    date_obs_end = date_plus_N(date_start, enrollment_period + observation_period)

    # convert everything into s3 date string format
    date_start = date_to_string(date_start)
    date_enroll_end = date_to_string(date_enroll_end)
    date_obs_end = date_to_string(date_obs_end)

    # prep events table
    events = spark.sql("select * from events")

    # ----------------- enrollment pings -----------------

    # get enrollments in the enrollment window for experiment
    enrollments = events.filter("event_category = 'normandy'")
    enrollments = enrollments.filter("event_method = 'enroll'")
    enrollments = enrollments.filter("submission_date_s3 >= '%s'" % date_start)
    enrollments = enrollments.filter("submission_date_s3 < '%s'" % date_enroll_end)
    enrollments = enrollments.filter("event_string_value = '%s'" % slug)

    # roll up to unique client_id, branch,
    #   if there are multiple pings, get the earliest enrollment.
    # also get the number of pings sent
    enrollments = enrollments.groupby([
        'client_id',
        'event_map_values.branch'
    ]).agg(
        F.min(F.col('submission_date_s3')).alias('submission_date_s3'),
        F.count('*').alias('ping_count_enrollment')
    )

    # rename columns for joining ease
    enrollments = enrollments.select([
        F.col('client_id').alias('client_id_enrollment'),
        F.col('branch').alias('branch_enrollment'),
        F.to_date(F.col('submission_date_s3'),
                  'yyyyMMdd').alias('first_branch_enrollment_dt'),
        F.col('ping_count_enrollment')
    ])

    # ------------ unenrollment pings --------------------

    # get unenrollments in the maximum observation period for experiment
    unenrollments = events.filter("event_category = 'normandy'")
    unenrollments = unenrollments.filter("event_method = 'unenroll'")
    unenrollments = unenrollments.filter("submission_date_s3 >= '%s'" % date_start)
    unenrollments = unenrollments.filter("submission_date_s3 <= '%s'" % date_obs_end)
    unenrollments = unenrollments.filter("event_string_value = '%s'" % slug)

    # group by client_id, branch. if there are multiple records, use the earliest date,
    # and get the number of pings sent
    unenrollments = unenrollments.groupby([
        'client_id',
        'event_map_values.branch'
    ]).agg(
        F.min(F.col('submission_date_s3')).alias('submission_date_s3'),
        F.count('*').alias('ping_count_unenrollment')
    )

    # rename columns for ease of joining
    unenrollments = unenrollments.select([
        F.col('client_id').alias('client_id_unenrollment'),
        F.col('branch').alias('branch_unenrollment'),
        F.to_date(F.col('submission_date_s3'),
                  'yyyyMMdd').alias('first_branch_unenrollment_dt'),
        F.col('ping_count_unenrollment')
    ])

    # ----------------- membership table ---------------

    # join the enrollment and unenrollment table
    # left side is the enrollments
    # the following unenrollments are included:
    #     client_id matches an enrollment
    #     branch matches an enrollment (if client_id exists
    #       but the branch doesn't match, it is ignored)
    #     unenrollment ping sent within observation period
    #       starting from enrollment date
    membership_table = enrollments.join(
        unenrollments,
        (F.col('client_id_enrollment') == F.col('client_id_unenrollment'))
        & (F.col('branch_enrollment') == F.col('branch_unenrollment'))
        & (F.datediff(F.col('first_branch_unenrollment_dt'),
                      F.col('first_branch_enrollment_dt')
                      ) < observation_period),
        how='left'
        )

    # rename columns to final form
    membership_table = membership_table.select([
        F.col('client_id_enrollment').alias('client_id'),
        F.col('branch_enrollment').alias('branch'),
        F.col('first_branch_enrollment_dt').alias('enrollment_dt'),
        F.col('ping_count_enrollment'),
        F.col('first_branch_unenrollment_dt').alias('unenrollment_dt'),
        F.col('ping_count_unenrollment'),
    ])

    return membership_table


def ms_pings_subset_df(df, date_start, total_period, columns=MS_USAGE_COLS, slug=None):
    """
    get subset of main summary that covers period of interest and columns of interest
      note: will convert submission_date_s3 to a date type column (activity_dt)
      note: if slug is provided, will only return pings tagged with the experiment and
            also provide the branch as a column

    table schema
      client_id
      activity_dt
      branch (optional)
      usage cols
    """
    # function can take datetime or the s3 date string
    if type(date_start) == str:
        date_start = string_to_date(date_start)

    # get date end of maximum possible observation period
    date_obs_end = date_plus_N(date_start, total_period)

    # convert everything into s3 date string format
    date_start = date_to_string(date_start)
    date_obs_end = date_to_string(date_obs_end)

    # ----------------- subset dates and columns -----------------

    df = df.filter("submission_date_s3 >= '%s'" % date_start)
    df = df.filter("submission_date_s3 <= '%s'" % date_obs_end)

    columns = ['client_id', 'submission_date_s3', 'experiments'] + columns
    df = df.select(columns)

    # ----------------- tagged only if slug provided -----------------

    if slug is not None:
        df = df.filter("experiments['%s'] is not null" % slug)
        df = df.withColumn('branch', F.col('experiments')[slug])

    # ----------------- clean up columns -----------------

    df = df.withColumn('activity_dt',
                       F.to_date(
                          F.col('submission_date_s3'),
                          'yyyyMMdd')
                       )
    df = df.drop("experiments")
    df = df.drop("submission_date_s3")

    return df


def as_pings_subset_df(as_df, date_start, total_period, slug=None):
    """
    get subset of activity stream pings with some columns standardized

    providing an experiment slug will add a branch field and remove pings
    without the slug

    table schema
      client_id
      activity_dt
      branch (optional)
      as cols

    """

    # function can take datetime or the s3 date string or as date string
    if type(date_start) == str:
        if '-' in date_start:
            date_start = string_to_date(date_start, '%Y-%m-%d')
        else:
            date_start = string_to_date(date_start)

    # get date end of maximum possible observation period
    date_obs_end = date_plus_N(date_start, total_period)

    # convert everything into as date string format
    date_start = date_to_string(date_start, '%Y-%m-%d')
    date_obs_end = date_to_string(date_obs_end, '%Y-%m-%d')

    # ----------------- subset dates -----------------

    as_df = as_df.filter("date >= '%s'" % date_start)
    as_df = as_df.filter("date <= '%s'" % date_obs_end)

    # ----------------- tagged only if slug provided -----------------

    if slug is not None:
        # set up udf for parsing activity stream experiment field
        schema = MapType(StringType(), StringType())
        as_experiment_field_udf = udf(as_experiment_field, schema)

        # get experiments field into standard format
        as_df = as_df.withColumn('experiments',
                                 as_experiment_field_udf(F.col('shield_id'))
                                 )

        # keep only data tagged with experiment and get branch column
        as_df = as_df.filter("experiments['%s'] is not null" % slug)
        as_df = as_df.withColumn('branch', F.col('experiments')[slug])
        as_df = as_df.drop('experiments')

    as_df = as_df.withColumn('activity_dt', F.col('date'))
    as_df = as_df.drop('shield_id').drop('date')

    return as_df


def experiment_pings(pings_df, membership_df, observation_period):
    """
    get the subset of pings that are:
        1: in the membership df (by client_id)
        2: activity date is after enrollment
           (at least the day after) and before end of observation period

    note: if the pings_df has a branch column, join on (client_id, branch)
    """

    # rename column so joining is easier
    pings_df = pings_df.withColumn(
        'client_id_pings',
        F.col('client_id')
                                  ).drop('client_id')

    # if branch exists, join on client_id, branch, date range
    if 'branch' in pings_df.columns:
        pings_df = pings_df.withColumn(
            'branch_pings',
            F.col('branch')
                                       ).drop('branch')
        df = membership_df.join(
            pings_df,
            (F.col('client_id') == F.col('client_id_pings'))
            & (F.col('branch') == F.col('branch_pings'))
            & (F.datediff(F.col('activity_dt'),
                          F.col('enrollment_dt')) < observation_period)
            & (F.datediff(F.col('activity_dt'),
                          F.col('enrollment_dt')) > 0),  # no enroll day pings
            how='left'
            )
        df = df.drop('branch_pings')

    else:
        df = membership_df.join(
            pings_df,
            (F.col('client_id') == F.col('client_id_pings'))
            & (F.datediff(F.col('activity_dt'),
                          F.col('enrollment_dt')) < observation_period)
            & (F.datediff(F.col('activity_dt'),
                          F.col('enrollment_dt')) > 0),  # no enroll day pings
            how='left')

    # clean up columns
    df = df.drop('client_id_pings')

    return df
