SELECT
  'feature' AS measure_type,
  feature_type AS cohort_level,
  feature_name AS cohort_name,
  os,
  country,
  client_id,
  MIN(submission_date) AS cohort_date,
  DATE '{start_date}' AS execution_date
FROM
  `{project}.{dataset}.{src}`
WHERE
  feature_type IN ('Feature',
    'Vertical')
  AND feature_name not in ('Others','feature: others')
  AND submission_date >= DATE '2018-11-01'
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6
-- LIMIT 0