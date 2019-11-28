with feature_session_event as (
  select client_id,
         country,
         submission_date,
         submission_timestamp,
         event_timestamp,
         event_vertical,
         feature_type,
         feature_name,
         session_time,
         url_counts,
         app_link_install,
         app_link_open,
         show_keyboard,
         count(1) -- dedup extra
  from `{project}.{dataset}.{src}`
  where submission_date = DATE '{start_date}'
  group by
    client_id,
    country,
    submission_date,
    submission_timestamp,
    event_timestamp,
    event_vertical,
    feature_type,
    feature_name,
    session_time,
    url_counts,
    app_link_install,
    app_link_open,
    show_keyboard
),

feature_session as (
  select
    client_id,
    country,
    submission_date,
    event_vertical,
    feature_type,
    feature_name,
    sum(session_time) as session_time,
    sum(url_counts) as url_counts,
    sum(app_link_install) as app_link_install,
    sum(app_link_open) as app_link_open,
    sum(show_keyboard) as show_keyboard
  from feature_session_event
  where feature_type = 'Feature'
  and (
    feature_name like 'feature:%content_tab%'
    or feature_name like 'feature:%tab_swipe%'
    or feature_name like '%category%'
    or feature_name like '%subcategory%'
    or feature_name like 'feed%'
    or feature_name like '%component_id%'
  )
  group by
    client_id,
    country,
    submission_date,
    event_vertical,
    feature_type,
    feature_name
),

vertical_session_event as (
  select
    client_id,
    country,
    submission_date,
    submission_timestamp,
    event_timestamp,
    DATETIME_ADD(DATETIME(submission_timestamp), INTERVAL event_timestamp MILLISECOND) as start_timestamp,
    event_method,
    event_object,
    event_value,
    extra_key,
    extra_value,
    event_vertical,
    feature_type,
    feature_name,
    LEAD (DATETIME_ADD(DATETIME(submission_timestamp), INTERVAL event_timestamp MILLISECOND), 1)
          OVER (PARTITION BY client_id, event_value, event_vertical, country
          ORDER BY DATETIME_ADD(DATETIME(submission_timestamp), interval event_timestamp MILLISECOND)) AS end_timestamp
from  `{project}.{dataset}.{src}`
  where event_method in ('start', 'end')
  and event_object = 'process'
  and feature_type = 'Vertical'
  and submission_date = DATE '{start_date}'
  --and submission_date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)  --param
),

vertical_session_time as (
  select
    client_id,
    country,
    submission_date,
    event_vertical,
    feature_type,
    feature_name,
    sum(case
          when DATETIME_DIFF(end_timestamp, start_timestamp, MILLISECOND) > 30*60*1000 then 0 -- discard session time >= 30min
          else DATETIME_DIFF(end_timestamp, start_timestamp, MILLISECOND)
        end) as session_time
  from vertical_session_event
  where event_method = 'start'
  group by
    client_id,
    country,
    submission_date,
    event_vertical,
    feature_type,
    feature_name
),

browser_search AS (
  SELECT
    client_id,
    metadata.geo_country AS country,
    submission_date,
    'all' AS event_vertical,
    SUM(t.value) AS search_counts
  FROM
    `{project}.{dataset}.{src2}`
  CROSS JOIN
    UNNEST(searches) AS t
  WHERE t.value < 10000
  and submission_date = DATE '{start_date}'
  GROUP BY
    1,
    2,
    3,
    4),

vertical_session_others as (
  select
    client_id,
    country,
    submission_date,
    event_vertical,
    sum(url_counts) as url_counts,
    sum(app_link_install) as app_link_install,
    sum(app_link_open) as app_link_open,
    sum(show_keyboard) as show_keyboard
  from feature_session
  group by
    client_id,
    country,
    submission_date,
    event_vertical
),

vertical_session as (
  select
    t.client_id,
    t.country,
    t.submission_date,
    t.event_vertical,
    t.feature_type,
    t.feature_name,
    session_time,
    CASE WHEN t.feature_type = 'Vertical' AND t.event_vertical = 'all' THEN search_counts ELSE url_counts END AS url_counts,
    app_link_install,
    app_link_open,
    show_keyboard
  from vertical_session_time as t
  left join vertical_session_others as o
  on t.client_id = o.client_id
  and t.country = o.country
  and t.submission_date = o.submission_date
  and t.event_vertical = o.event_vertical
  left join browser_search as b
  on t.client_id = b.client_id
  and t.country = b.country
  and t.submission_date = b.submission_date
  and t.event_vertical = b.event_vertical
),

app_session as (
  select
    client_id,
    country,
    submission_date,
    'all' as event_vertical,
    'App' as feature_type,
    'App' as feature_name,
    sum(session_time) as session_time,
    sum(url_counts) as url_counts,
    sum(app_link_install) as app_link_install,
    sum(app_link_open) as app_link_open,
    sum(show_keyboard) as show_keyboard
  from vertical_session
  group by
    client_id,
    country,
    submission_date
)


select *
from feature_session

union all

select *
from vertical_session

union all

select *
from app_session


