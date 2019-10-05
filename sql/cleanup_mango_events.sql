DELETE
  `{project}.{dataset}.{dest}`
WHERE
  normalized_app_name='Zerda' AND
  DATE(submission_timestamp) >= DATE '{start_date}' AND
  DATE(submission_timestamp) < DATE_ADD(DATE '{start_date}', INTERVAL '1' DAY)
