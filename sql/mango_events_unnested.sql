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
  CASE WHEN f0_ IS NULL THEN 0 ELSE f0_ END AS event_timestamp,
  CASE WHEN f1_ IS NULL THEN "" ELSE f1_ END AS event_category,
  CASE WHEN f2_ IS NULL THEN "" ELSE f2_ END AS event_method,
  CASE WHEN f3_ IS NULL THEN "" ELSE f3_ END AS event_object,
  CASE WHEN f4_ IS NULL THEN "" ELSE f4_ END AS event_value,
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