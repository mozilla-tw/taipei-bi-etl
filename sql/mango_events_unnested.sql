WITH
  events AS (
  SELECT
    * EXCEPT(additional_properties,
    events,
    f0_,
    f1_,
    f2_,
    f3_,
    f4_,
    f5_),
  f0_ AS event_timestamp,
  f1_ AS event_category,
  f2_ AS event_method,
  f3_ AS event_object,
  f4_ AS event_value,
  f5_ AS event_extra
  FROM
    `{project}.{dataset}.{src}`,
    UNNEST(events)
--     UNNEST(`{project}.{dataset}`.udf_js_json_extract_events(JSON_EXTRACT(additional_properties, '$.events'))) AS event
  WHERE
    normalized_app_name = 'Zerda'
    AND normalized_channel = 'release' )
SELECT
  *
FROM
  events