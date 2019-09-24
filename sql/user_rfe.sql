
WITH events AS (
    SELECT 
      GENERATE_UUID() as id,
      *
    FROM
      `moz-fx-data-shared-prod.telemetry.focus_event`,
      UNNEST(udf_js_json_extract_events(JSON_EXTRACT(additional_properties, '$.events'))) AS event
    WHERE
      DATE(submission_timestamp) >= DATE '{start_date}' AND
      DATE(submission_timestamp) <= DATE '{end_date}' AND
      normalized_app_name = 'Zerda' AND
      normalized_channel = 'release'
)

SELECT 
       id, 
       client_id, submission_timestamp, tz, 
       event_method, event_object, event_value, 
       extra.key as extra_key, extra.value as extra_value,
       settings.key as settings_key, settings.value as settings_value
FROM
  events
LEFT JOIN
  UNNEST(event_extra) as extra,
  UNNEST(settings) as settings
LIMIT 1000