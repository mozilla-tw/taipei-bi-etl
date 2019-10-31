with feature_session_event as (
  select client_id,
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
  where submission_date >= DATE_SUB(DATE('{start_date}'), INTERVAL 27 DAY) -- 取 partition
  and submission_date < DATE_ADD(DATE('{start_date}'), INTERVAL 1 DAY)
  group by
    client_id,
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
    event_vertical,
    feature_type,
    feature_name,
    sum(session_time) as session_time,
    sum(url_counts) as url_counts,
    sum(app_link_install) as app_link_install,
    sum(app_link_open) as app_link_open,
    sum(show_keyboard) as show_keyboard,
    DATE('{start_date}') as execution_date -- 今天跑昨天以前的資料
  from feature_session_event
  where feature_type = 'Feature'
  and feature_name like 'feature: %'
  and (feature_name like '%content_tab%' or feature_name like '%tab_swipe%')
  group by
    client_id,
    event_vertical,
    feature_type,
    feature_name
),

vertical_session_event as (
  select
    client_id,
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
    LEAD (DATETIME_ADD(DATETIME(submission_timestamp), INTERVAL event_timestamp MILLISECOND), 1) OVER (PARTITION BY client_id, event_value, event_vertical ORDER BY submission_timestamp) AS end_timestamp
  from  `{project}.{dataset}.{src}`
  where submission_date >= DATE_SUB(DATE('{start_date}'), INTERVAL 27 DAY) -- 取 partition
  and submission_date < DATE_ADD(DATE('{start_date}'), INTERVAL 1 DAY)
  and event_method in ('start', 'end')
  and event_object = 'process'
  and feature_type = 'Vertical'
),

vertical_session_time as (
  select
    client_id,
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
    event_vertical,
    feature_type,
    feature_name
),

vertical_session_others as (
  select
    client_id,
    event_vertical,
    sum(url_counts) as url_counts,
    sum(app_link_install) as app_link_install,
    sum(app_link_open) as app_link_open,
    sum(show_keyboard) as show_keyboard
  from feature_session
  group by
    client_id,
    event_vertical
),

vertical_session as (
  select
    t.client_id,
    t.event_vertical,
    t.feature_type,
    t.feature_name,
    session_time,
    url_counts,
    app_link_install,
    app_link_open,
    show_keyboard,
    DATE('{start_date}') as execution_date
  from vertical_session_time as t
  left join vertical_session_others as o
  on t.client_id = o.client_id and t.event_vertical = o.event_vertical
),

app_session as (
  select
    client_id,
    'all' as event_vertical,
    'App' as feature_type,
    'App' as feature_name,
    sum(session_time) as session_time,
    sum(url_counts) as url_counts,
    sum(app_link_install) as app_link_install,
    sum(app_link_open) as app_link_open,
    sum(show_keyboard) as show_keyboard,
    execution_date
  from vertical_session
  group by
    client_id,
    execution_date
)

select *
from feature_session

union all

select *
from vertical_session

union all

select *
from app_session