SELECT
  *,
  DATE(submission_timestamp) AS submission_date
FROM
  `{src}`
WHERE
  normalized_app_name='Zerda' AND
  DATE(submission_timestamp) >= DATE '2018-11-01' AND
  DATE(submission_timestamp) < DATE_ADD(DATE '{start_date}', INTERVAL '1' DAY)
