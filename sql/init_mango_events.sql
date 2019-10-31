SELECT
    additional_properties,
    arch,
    client_id,
    created,
    created_date,
    created_timestamp,
    device,
    document_id,
    experiments,
    locale,
    metadata,
    normalized_app_name,
    normalized_channel,
    normalized_country_code,
    normalized_os,
    normalized_os_version,
    os,
    osversion,
    process_start_timestamp,
    profile_date,
    sample_id,
    seq,
    settings,
    submission_timestamp,
    tz,
    v,
    events,
    DATE(submission_timestamp) AS submission_date
FROM
  `{src}`
WHERE
  normalized_app_name='Zerda' AND
  DATE(submission_timestamp) >= DATE '2018-11-01' AND
  DATE(submission_timestamp) < DATE_ADD(DATE '{start_date}', INTERVAL '1' DAY)
