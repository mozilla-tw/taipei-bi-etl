DELETE
  `{project}.{dataset}.{dest}`
WHERE
  normalized_app_name='Zerda' AND
  DATE(submission_timestamp) = DATE '{start_date}'
