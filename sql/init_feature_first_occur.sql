SELECT
  CONCAT('feature_', LOWER(feature_type)) AS cohort_type,
  feature_name AS cohort_name,
  os,
  country,
  client_id,
  MIN(submission_date) AS cohort_date
FROM
  `{src}`
WHERE
  feature_type IN ('Feature',
    'Vertical')
  AND feature_name <> 'Others'
  AND submission_date = DATE '{start_date}'
GROUP BY
  1,
  2,
  3,
  4,
  5
LIMIT 0