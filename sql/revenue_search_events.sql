WITH events AS (
SELECT
  client_id,
  TIMESTAMP_TRUNC(submission_timestamp, DAY) AS day,
  normalized_country_code AS country_code,
  normalized_os AS os,
  additional_properties
FROM
  `{project}.{dataset}.focus_event`
WHERE
  DATE(submission_timestamp) >= DATE '{start_date}' AND
  DATE(submission_timestamp) <= DATE '{end_date}' AND
  normalized_app_name = 'Zerda' AND
  normalized_channel = 'release'
)
SELECT
  day,
  country_code,
  os,
  count(client_id) AS event_count
FROM
  events
CROSS JOIN
  UNNEST(udf_js_json_extract_events(JSON_EXTRACT(additional_properties, '$.events'))) AS event
WHERE
  event.event_method IN('type_query', 'select_query')
GROUP BY 1, 2, 3