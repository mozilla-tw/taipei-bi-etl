WITH
flatten_event AS (
  SELECT
    client_id,
    submission_timestamp,
    tz,
    os,
    metadata.geo.country,
    event_method, event_object, event_value,
    case when extra.key is null then '' else lower(extra.key) end AS extra_key,
    case when extra.value is null then '' else lower(extra.value) end AS extra_value,
    case when settings.key is null then '' else lower(settings.key) end AS settings_key,
    case when settings.value is null then '' else lower(settings.value) end AS settings_value
  FROM
    `{project}.{dataset}.{src}`
  LEFT JOIN
    UNNEST(event_extra) AS extra,
    UNNEST(settings) AS settings
),

mapping AS (
  SELECT client_id,
         submission_timestamp,
         tz,
         os,
         country,
         event_method, event_object, event_value,
         extra_key, extra_value,
         settings_key, settings_value,
         `rocket-dev01`.mango_dev.udf_js_feature_mapping(
            event_method, event_object, event_value,
            extra_key, extra_value,
            settings_key, settings_value) as map
  FROM flatten_event
)

--

select client_id,
       submission_timestamp,
       tz,
       os,
       country,
       event_method, event_object, event_value,
       extra_key, extra_value,
       settings_key, settings_value,
       'Feature' as feature_type, feature_item as feature_name
from mapping,
unnest(map.feature) as feature_item

UNION ALL
select client_id,
       submission_timestamp,
       tz,
       os,
       country,
       event_method, event_object, event_value,
       extra_key, extra_value,
       settings_key, settings_value,
       'Vertical' as feature_type, map.vertical as feature_name
from mapping

UNION ALL
select client_id,
       submission_timestamp,
       tz,
       os,
       country,
       event_method, event_object, event_value,
       extra_key, extra_value,
       settings_key, settings_value,
       'App' as feature_type, map.app as feature_name
from mapping
