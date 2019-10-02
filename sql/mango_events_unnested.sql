WITH
  events AS (
  SELECT
    * EXCEPT(additional_properties)
  FROM
    `{project}.{dataset}.{src}`,
    UNNEST(`{project}.{dataset}`.udf_js_json_extract_events(JSON_EXTRACT(additional_properties,
          '$.events'))) AS event
  WHERE
    normalized_app_name = 'Zerda'
    AND normalized_channel = 'release' )
SELECT
  *
FROM
  events