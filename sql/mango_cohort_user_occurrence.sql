SELECT
  uo.os,
  uo.country,
  'channel' AS measure_type,
  'Network' AS cohort_level,
  uc.network_name AS cohort_name,
  uc.client_id,
  uo.cohort_date,
  uo.occur_date,
  uo.occur_day,
  uo.occur_week,
  uo.occur_month
FROM
  `{project}.{dataset}.{src3}` AS uo
LEFT JOIN
  `{project}.{dataset}.{src2}` AS uc ON uc.client_id=uo.client_id
WHERE
  cohort_date IS NOT NULL
  AND uo.cohort_level = 'App'
UNION ALL
SELECT
  ufo.os, ufo.country,
  'feature' AS measure_type,
  ufo.cohort_level,
  ufo.cohort_name,
  ufo.client_id,
  ufo.cohort_date,
  ufo.occur_date,
  ufo.occur_day,
  ufo.occur_week,
  ufo.occur_month
FROM
  `{project}.{dataset}.{src3}` AS ufo
WHERE
  cohort_date IS NOT NULL