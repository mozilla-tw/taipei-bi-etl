SELECT
  TIMESTAMP_TRUNC(submission_timestamp, DAY) AS day,
  count(client_id) AS events
FROM
  `{project}.{dataset}.focus_event`
CROSS JOIN
  UNNEST(udf_js_json_extract_events(JSON_EXTRACT(additional_properties, '$.events'))) AS event
WHERE
  DATE(submission_timestamp) >= DATE '{from_date}' AND
  DATE(submission_timestamp) <= DATE '{to_date}' AND
  normalized_app_name = 'Zerda' AND
  normalized_channel = 'release' AND
  event.event_method IN('type_query', 'select_query')
GROUP BY 1