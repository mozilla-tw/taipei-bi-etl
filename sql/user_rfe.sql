
WITH events AS (
    SELECT 
      FARM_FINGERPRINT(CONCAT(
            CAST(DATE(submission_timestamp) as STRING), 
            metadata.geo.country,
            CAST(ROW_NUMBER() OVER(PARTITION BY DATE(submission_timestamp), metadata.geo.country) as STRING)
        )) as id,
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
left join 
  UNNEST(event_extra) as extra,
  UNNEST(settings) as settings
limit 10
