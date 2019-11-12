WITH
  existing AS (
  SELECT
    os, country, measure_type, cohort_level, cohort_name, client_id
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
  MIN(fm.submission_date) AS cohort_date,
  DATE '{start_date}' AS execution_date
FROM
  `{project}.{dataset}.{src}` AS fm
LEFT JOIN
  existing AS ec
ON
  fm.client_id=ec.client_id
  AND fm.feature_type=ec.cohort_level
  AND 'feature'=ec.measure_type
  AND fm.feature_type=ec.cohort_level
  AND fm.feature_name=ec.cohort_name
  AND fm.os=ec.os
  AND fm.country=ec.country
WHERE
  feature_type IN ('Feature',
    'Vertical')
  AND feature_name not in ('Others','feature: others')
  AND submission_date >= DATE '2018-11-01'
  AND fm.country IS NOT NULL
  AND fm.os IS NOT NULL
  AND ec.client_id IS NULL
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6
