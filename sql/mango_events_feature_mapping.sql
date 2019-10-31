WITH
event_set as (
  select
    client_id,
    submission_timestamp,
    submission_date,
    tz,
    os,
    metadata.geo.country,
    (SELECT value FROM UNNEST(settings) WHERE key = 'pref_search_engine') AS settings_search_engine,
    event_timestamp,
    event_method,
    event_object,
    event_value,
    event_extra,
    (SELECT value FROM UNNEST(event_extra) WHERE key = 'vertical') AS event_vertical,
    (SELECT value FROM UNNEST(event_extra) WHERE key = 'session_time') AS session_time,
    (SELECT value FROM UNNEST(event_extra) WHERE key = 'url_counts') AS url_counts,
    (SELECT 1 as a FROM UNNEST(event_extra) WHERE key = 'app_link' and value = 'install') AS app_link_install,
    (SELECT 1 as a FROM UNNEST(event_extra) WHERE key = 'app_link' and value = 'open') AS app_link_open,
    (SELECT 1 as a FROM UNNEST(event_extra) WHERE key = 'show_keyboard' and value = 'true') AS show_keyboard
  from
    `{project}.{dataset}.{src}`
),

flatten_extra AS (
  SELECT
    client_id,
    submission_timestamp,
    submission_date,
    tz,
    os,
    country,
    if(settings_search_engine is null, '', lower(settings_search_engine)) AS settings_search_engine,
    event_timestamp,
    event_method,
    event_object,
    event_value,
    if(extra.key is null, '', lower(extra.key)) AS extra_key,
    if(extra.value is null, '', lower(extra.value)) AS extra_value,
    if(event_vertical is null, '', lower(event_vertical)) AS event_vertical,
    cast(session_time as int64) as session_time,
    cast(url_counts as int64) as url_counts,
    cast(app_link_install as int64) as app_link_install,
    cast(app_link_open as int64) as app_link_open,
    cast(show_keyboard as int64) as show_keyboard
  FROM
    event_set
  LEFT JOIN
    UNNEST(event_extra) AS extra
),

mapping AS (
  SELECT
    client_id,
    submission_timestamp,
    submission_date,
    tz,
    os,
    country,
    settings_search_engine,
    event_timestamp,
    event_method,
    event_object,
    event_value,
    extra_key,
    extra_value,
    event_vertical,
    session_time,
    url_counts,
    app_link_install,
    app_link_open,
    show_keyboard,
    `{project}.{dataset}`.udf_js_feature_mapping(
        event_method,
        event_object,
        event_value,
        extra_key,
        extra_value,
        event_vertical,
        settings_search_engine) as map
  FROM
    flatten_extra
)

--

select
  client_id,
  submission_timestamp,
  submission_date,
  tz,
  os,
  country,
  settings_search_engine,
  event_timestamp,
  event_method,
  event_object,
  event_value,
  extra_key,
  extra_value,
  event_vertical,
  session_time,
  url_counts,
  app_link_install,
  app_link_open,
  show_keyboard,
  'Feature' as feature_type,
  feature_item as feature_name
from
  mapping,
  unnest(map.feature) as feature_item

UNION ALL
select
  client_id,
  submission_timestamp,
  submission_date,
  tz,
  os,
  country,
  settings_search_engine,
  event_timestamp,
  event_method,
  event_object,
  event_value,
  extra_key,
  extra_value,
  event_vertical,
  session_time,
  url_counts,
  app_link_install,
  app_link_open,
  show_keyboard,
 'Vertical' as feature_type,
  map.vertical as feature_name
from
  mapping

UNION ALL
select
  client_id,
  submission_timestamp,
  submission_date,
  tz,
  os,
  country,
  settings_search_engine,
  event_timestamp,
  event_method,
  event_object,
  event_value,
  extra_key,
  extra_value,
  event_vertical,
  session_time,
  url_counts,
  app_link_install,
  app_link_open,
  show_keyboard,
  'App' as feature_type,
  map.app as feature_name
from mapping