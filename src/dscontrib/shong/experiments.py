# experiment_membership_df(slug, date_start, enrollment_period, observation_period)
# ms_pings_subset_df(df, date_start, total_period, columns=MS_USAGE_COLS, slug=None)
# as_pings_subset_df(as_df, date_start, total_period, slug=None)
# experiment_pings_df(pings_df, membership_df, observation_period)
# daily_usage_df(df, agg_functions, client_fields=client_fields)
# agg functions:
#   all:
#       - client_fields
#   main summary:
#       - daily_usage_aggs
#   as health:
#       - as_health_aggs
#   as sessions:
#       - as_session_aggs
#   as clicks:
#       - as_clicks_aggs
#   as snippets:
#       - as_snippets_aggs = get_as_snippets_aggs(message_id=None)

# spark imports
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import MapType, StringType, BooleanType

# local imports
from .constants import MS_USAGE_COLS
from .activitystream import as_experiment_field, as_pref_setting
from .activitystream import as_health_default_homepage, as_health_default_newtab
from .util import date_plus_N, date_to_string, string_to_date


# fetch spark context
spark = SparkSession.builder.getOrCreate()


# ------------------- experiment membership ------------------

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


# ------------------- pre-process pings ------------------

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
    date_start_str = date_to_string(date_start)
    date_obs_end_str = date_to_string(date_obs_end)

    # if we're looking back, switch start and end
    if date_obs_end < date_start:
        date_start_str = date_to_string(date_obs_end)
        date_obs_end_str = date_to_string(date_start)

    # ----------------- subset dates and columns -----------------

    df = df.filter("submission_date_s3 >= '%s'" % date_start_str)
    df = df.filter("submission_date_s3 <= '%s'" % date_obs_end_str)

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
    date_start_str = date_to_string(date_start, '%Y-%m-%d')
    date_obs_end_str = date_to_string(date_obs_end, '%Y-%m-%d')

    # if we're looking back, switch start and end
    if date_obs_end < date_start:
        date_start_str = date_to_string(date_obs_end, '%Y-%m-%d')
        date_obs_end_str = date_to_string(date_start, '%Y-%m-%d')

    # ----------------- subset dates -----------------

    as_df = as_df.filter("date >= '%s'" % date_start_str)
    as_df = as_df.filter("date <= '%s'" % date_obs_end_str)

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


# ------------------- keep only experiment pings ------------------

def experiment_pings_df(pings_df, membership_df, observation_period):
    """
    get the subset of pings that are:
        1: in the membership df (by client_id)
        2: activity date is after enrollment
           (at least the day after) and before end of observation period

    note: if the pings_df has a branch column, join on (client_id, branch)
    """
    # if observation period is negative, get the days preceding
    # if positive, get days following
    top = observation_period
    bottom = 0
    if observation_period < 0:
        top = 0
        bottom = observation_period

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
                          F.col('enrollment_dt')) < top)
            & (F.datediff(F.col('activity_dt'),
                          F.col('enrollment_dt')) > bottom),  # no enroll day pings
            how='left'
            )
        df = df.drop('branch_pings')

    else:
        df = membership_df.join(
            pings_df,
            (F.col('client_id') == F.col('client_id_pings'))
            & (F.datediff(F.col('activity_dt'),
                          F.col('enrollment_dt')) < top)
            & (F.datediff(F.col('activity_dt'),
                          F.col('enrollment_dt')) > bottom),  # no enroll day pings
            how='left')

    # clean up columns
    df = df.drop('client_id_pings')

    return df


# ------------------- client-daily rollups ------------------

client_fields = [
        F.last(F.col('enrollment_dt')).alias('enrollment_dt'),
        F.last(F.col('ping_count_enrollment')).alias('ping_count_enrollment'),
        F.last(F.col('unenrollment_dt')).alias('unenrollment_dt'),
        F.last(F.col('ping_count_unenrollment')).alias('ping_count_unenrollment')
    ]


# generic client-daily rollup function
def daily_usage_df(df, agg_functions, client_fields=client_fields):
    """
    generic function for rolling up experiment pings into a
    (client_id, branch, activity date) level
        note: the client_fields above are assumed to be provided by default
        note: you have to provide the agg functions and decide the roll up
              logic inside them individually
    """
    if client_fields:
        agg_functions = client_fields + agg_functions
    return df.groupby(['client_id', 'branch', 'activity_dt']).agg(*agg_functions)


# ------------------- client-daily agg functions ------------------

# agg functions:
#
# all:
#   client_fields
# main summary:
#   - daily_usage_aggs
# as health:
#   - as_health_aggs
# as sessions:
#   - as_session_aggs
# as clicks:
#   - as_clicks_aggs
# as snippets:
#   - as_snippets_aggs = get_as_snippets_aggs(message_id=None)


# agg fields: main summary usage ------------------

days_used = F.countDistinct(
                    F.col('activity_dt')).alias('days_used')

active_hours = F.sum(
                F.coalesce(
                    F.col('active_ticks'),
                    F.lit(0)
                          ) * F.lit(5) / F.lit(3600)).alias('active_hours')

uris = F.sum(
        F.coalesce(
            F.col('scalar_parent_browser_engagement_total_uri_count'),
            F.lit(0)
                  )).alias('uris')

tabs_opened = F.sum(
                F.coalesce(
                    F.col('scalar_parent_browser_engagement_tab_open_event_count'),
                    F.lit(0)
                          )).alias('tabs_opened')

windows_opened = F.sum(
                   F.coalesce(
                     F.col('scalar_parent_browser_engagement_window_open_event_count'),
                     F.lit(0)
                             )).alias('windows_opened')

ping_count = F.count(F.col('activity_dt')).alias('usage_ping_count')


daily_usage_aggs = [
        active_hours,
        uris,
        tabs_opened,
        windows_opened,
        ping_count
    ]

# agg fields: as health ping ----------------------------------

schema = BooleanType()
as_health_default_homepage_udf = udf(as_health_default_homepage, schema)

schema = BooleanType()
as_health_default_newtab_udf = udf(as_health_default_newtab, schema)


# note, we're just checking if these are set to true at ANY point in the day
# so if a user has it set to true, and then sets it to false, it'll show up at true
as_health_aggs = [
    F.max(
        as_health_default_homepage_udf(F.col('value'))
        ).alias('has_default_homepage'),
    F.max(
        as_health_default_newtab_udf(F.col('value'))
        ).alias('has_default_newtb'),
    F.count('activity_dt').alias('as_health_ping_count')
]

# agg fields: as session ping ----------------------------------

schema = BooleanType()
as_pref_setting_udf = udf(as_pref_setting, schema)

# note, we're just checking if these are set to true at ANY point in the day
# so if a user has it set to true, and then sets it to false, it'll show up at true
as_session_aggs = [
    F.max(
        as_pref_setting_udf(F.col('user_prefs'), F.lit(1))
        ).alias('has_search'),
    F.max(
        as_pref_setting_udf(F.col('user_prefs'), F.lit(2))
        ).alias('has_topsites'),
    F.max(
        as_pref_setting_udf(F.col('user_prefs'), F.lit(4))
        ).alias('has_pocket'),
    F.max(
        as_pref_setting_udf(F.col('user_prefs'), F.lit(8))
        ).alias('has_highlights'),
    F.max(
        as_pref_setting_udf(F.col('user_prefs'), F.lit(16))
        ).alias('has_snippets'),
    F.countDistinct('session_id').alias('as_sessions_count'),
    F.count('activity_dt').alias('as_session_ping_count')
]


# agg fields: as click ping ----------------------------------

TOPSITE_CONDITION = (F.col('source') == 'TOP_SITES')
HIGHLIGHT_CONDITION = (F.col('source') == 'HIGHLIGHTS')
HOMEPAGE_CONDITION = (F.col('page') == 'about:home')
NEWTAB_CONDITION = (F.col('page') == 'about:newtab')

# note: don't deal with nulls, if they didn't do it, they get a 0
# note: the generic click types can include about:welcome, but
#       the page specific ones don't
as_clicks_aggs = [
    # topsites
    F.sum(F.when(
            TOPSITE_CONDITION,
            1).otherwise(0)
          ).alias('topsite_clicks'),
    F.sum(F.when(
            TOPSITE_CONDITION & HOMEPAGE_CONDITION,
            1).otherwise(0)
          ).alias('topsite_homepage_clicks'),
    F.sum(F.when(
            TOPSITE_CONDITION & NEWTAB_CONDITION,
            1).otherwise(0)
          ).alias('topsite_newtab_clicks'),
    # highlights
    F.sum(F.when(
            HIGHLIGHT_CONDITION,
            1).otherwise(0)
          ).alias('highlight_clicks'),
    F.sum(F.when(
            HIGHLIGHT_CONDITION & HOMEPAGE_CONDITION,
            1).otherwise(0)
          ).alias('highlight_homepage_clicks'),
    F.sum(F.when(
            HIGHLIGHT_CONDITION & NEWTAB_CONDITION,
            1).otherwise(0)
          ).alias('highlight_newtab_clicks'),
    # combined
    F.sum(F.when(
            (TOPSITE_CONDITION | HIGHLIGHT_CONDITION),
            1).otherwise(0)
          ).alias('combined_clicks'),
    F.sum(F.when(
            (TOPSITE_CONDITION | HIGHLIGHT_CONDITION) & HOMEPAGE_CONDITION,
            1).otherwise(0)
          ).alias('combined_homepage_clicks'),
    F.sum(F.when(
            (TOPSITE_CONDITION | HIGHLIGHT_CONDITION) & NEWTAB_CONDITION,
            1).otherwise(0)
          ).alias('combined_newtab_clicks')
]


# agg fields: as snippet ping ----------------------------------

# note: don't deal with nulls or page, if they did it, they
# get a 1, otherwise 0
# note: unlike the other aggs, which are statically defined
#       this is defined as a function you have to run to
#       to produce the aggs list
#       this is because we'll want to parameterize based
#       on the message_id we are interested in
def get_as_snippets_aggs(message_id=None):
    """
    given a message_id for a snippet, return the agg functions
    for how many times that snippet was shown, blocked, dismissed,
    or clicked.

    if no message_id provided, then returns agg functions for all
    snippets.

    note: returns a list of agg functions for use in:
            daily_usage_df(df, agg_functions, client_fields=client_fields)
    """
    ID_CONDITION = F.lit(True)
    field_str = ''
    if message_id:
        ID_CONDITION = (F.col('message_id') == message_id)
        field_str = message_id

    IMPRESSION_CONDITION = (F.col('event') == 'IMPRESSION')
    BLOCK_CONDITION = (F.col('event') == 'BLOCK')
    DISMISS_CONDITION = (F.col('event') == 'DISMISS')
    CLICK_BUTTON_CONDITION = (F.col('event') == 'CLICK_BUTTON')

    as_snippets_aggs = [
        F.sum(F.when(
            (IMPRESSION_CONDITION & ID_CONDITION),
            1
        ).otherwise(0)
                 ).alias('snippet_impression_' + field_str),
        F.sum(F.when(
            (BLOCK_CONDITION & ID_CONDITION),
            1
        ).otherwise(0)
                 ).alias('snippet_block_' + field_str),
        F.sum(F.when(
            (DISMISS_CONDITION & ID_CONDITION),
            1
        ).otherwise(0)
                 ).alias('snippet_dismiss_' + field_str),
        F.sum(F.when(
            (CLICK_BUTTON_CONDITION & ID_CONDITION),
            1
        ).otherwise(0)
                 ).alias('snippet_click_' + field_str),
    ]

    return as_snippets_aggs


# ------------------- full client-daily df ------------------

# we always expect these columns in the df
# for use with joining
COMMON_COLUMNS = [
                 'client_id',
                 'branch',
                 'activity_dt',
                 'enrollment_dt',
                 'ping_count_enrollment',
                 'unenrollment_dt',
                 'ping_count_unenrollment'
                 ]


def null_safe_join(df1, df2, join_cols=COMMON_COLUMNS):
    """
    join dfs with the behavior that joining columns that
    are null will be treated as equal (so we don't get)
    multiple rows when the dfs have nulls in the same
    columns
    """

    def rename_col(df, column_name):
        df = df.withColumn(column_name + '_temp', F.col(column_name))
        df = df.drop(F.col(column_name))
        return df

    def null_safe_condition(column_name):
        return F.col(column_name).eqNullSafe(F.col(column_name + '_temp'))

    def get_null_safe_conditions(cols):
        expression = null_safe_condition(cols[0])
        for column_name in cols[1:]:
            expression = expression & null_safe_condition(column_name)
        return expression

    for column_name in join_cols:
        df2 = rename_col(df2, column_name)

    df = df1.join(df2, get_null_safe_conditions(join_cols), how='full')

    for column_name in join_cols:
        df = df.withColumn(column_name,
                           F.coalesce(F.col(column_name),
                                      F.col(column_name + '_temp')
                                      )
                           )    # not sure this is necessary...
        df = df.drop(F.col(column_name + '_temp'))

    return df


def overall_client(dfs, join_cols=COMMON_COLUMNS):
    """
    get multiple (client_id, branch, activity_dt) dfs and
    join into single df
    """

    base_df = dfs[0]

    for df in dfs[1:]:
        base_df = null_safe_join(base_df, df, join_cols)

    return base_df


def cleanup_no_activity_rows(df, activity_field='activity_dt'):
    """
    each df that was joined can produce "empty" activity rows for
    clients that didn't have activity in that df's activity
    this can blow up so you have "empty" rows for clients that
    did have activity in one of the dfs. this just cleans it up
    so that (client_id, branch) combos that have zero activity
    only get one row
    """

    df_has_activity = df.filter("{} is not null".format(activity_field))\
                        .select([
                            F.col('client_id').alias('client_id_temp'),
                            F.col('branch').alias('branch_temp')
                                ]).distinct()

    df = df.join(df_has_activity,
                 F.isnull(F.col('activity_dt'))
                 & (F.col('client_id') == F.col('client_id_temp'))
                 & (F.col('branch') == F.col('branch_temp')),
                 how='left'
                 )
    df = df.filter(F.isnull(F.col('client_id_temp')))
    df = df.drop('client_id_temp').drop('branch_temp')

    return df


NULLABLE_COLS = [
    'client_id',
    'branch',
    'activity_dt',
    'enrollment_dt',
    'ping_count_enrollment',
    'unenrollment_dt',
    'ping_count_unenrollment',
    'has_default_homepage',
    'has_default_newtb',
    'has_search',
    'has_topsites',
    'has_pocket',
    'has_highlights',
    'has_snippets'
]


def cleanup_nulls_df(df, nullable_cols=NULLABLE_COLS):
    """
    fill nulls with 0 for all columns except for the nullable cols
    """

    def clean_nulls(column, clean=True):
        if clean:
            return F.coalesce(F.col(column), F.lit(0)).alias(column)
        else:
            return F.col(column)

    cleaned_cols = []
    for col in df.columns:
        if col in nullable_cols:
            cleaned_cols.append(clean_nulls(col, False))
        else:
            cleaned_cols.append(clean_nulls(col, True))

    return df.select(cleaned_cols)
