# my personal S3 directory
S3_ROOT = 's3://mozilla-metrics/user/shong/'


# activity stream data queries
AS_HEALTH_QUERY = """
      SELECT shield_id, client_id, date, value
      FROM ping_centre_main
      WHERE event = 'AS_ENABLED'
            AND date >= '{START_DT}' AND date <= '{END_DT}'
                  """

AS_SESSION_QUERY = """
    SELECT shield_id, client_id, date, session_id, user_prefs
    FROM assa_sessions_daily_by_client_id
    WHERE date >= '{START_DT}' AND date <= '{END_DT}'
                   """

AS_CLICKS_QUERY = """
  SELECT shield_id, client_id, date, source, page
  FROM assa_events_daily
  WHERE source IN ('TOP_SITES', 'HIGHLIGHTS')
        AND event = 'CLICK'
        AND date >= '{START_DT}' AND date <= '{END_DT}'
                  """

AS_SNIPPETS_QUERY = """
    SELECT shield_id, impression_id AS client_id, date, event, message_id
    FROM assa_router_events_daily
    WHERE source = 'snippets_user_event'
          AND event in ('IMPRESSION', 'BLOCK', 'CLICK_BUTTON', 'DISMISS')
          AND date >= '{START_DT}' AND date <= '{END_DT}'
                    """

# baseline main summary usage columns
MS_USAGE_COLS = ['active_ticks',
                 'search_counts',
                 'scalar_parent_browser_engagement_total_uri_count',
                 'scalar_parent_browser_engagement_tab_open_event_count',
                 'scalar_parent_browser_engagement_window_open_event_count',
                 'scalar_parent_browser_search_with_ads',
                 'scalar_parent_browser_search_ad_clicks',
                 'active_addons']
