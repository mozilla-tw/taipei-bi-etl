WITH
  existing_clients AS (
  SELECT
    DISTINCT client_id
  FROM
    `{project}.{dataset}.{dest}`
)

SELECT
  'feature' AS measure_type,
  fm.feature_type AS cohort_level,
  fm.feature_name AS cohort_name,
  fm.os,
  fm.country,
  fm.client_id,
  MIN(fm.submission_date) AS cohort_date
FROM
  `{src}` AS fm
LEFT JOIN
  existing_clients AS ec
ON
  fm.client_id=ec.client_id
WHERE
  feature_type IN ('Feature',
    'Vertical')
  AND feature_name not in ('Others','feature: others')
  AND submission_date = DATE '{start_date}'
  AND ec.client_id IS NULL
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6
