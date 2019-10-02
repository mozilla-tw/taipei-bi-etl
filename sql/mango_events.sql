SELECT
  *
FROM
  `{project}.{dataset}.{table}`
WHERE
  normalized_app_name='Zerda' AND
  DATE(submission_timestamp) >= DATE '{start_date}' AND
  DATE(submission_timestamp) <= DATE '{end_date}'
