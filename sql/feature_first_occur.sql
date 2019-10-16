SELECT
  CONCAT('feature_', LOWER(fm.feature_type)) AS cohort_type,
  fm.feature_name AS cohort_name,
  fm.os,
  fm.country,
  fm.client_id,
  MIN(fm.submission_date) AS cohort_date
FROM
  `{src}` AS fm
LEFT JOIN
  `{project}.{dataset}.{dest}` AS fo
ON
  fm.client_id=fo.client_id
  AND fm.feature_name=fo.cohort_name
  AND CONCAT('feature_', LOWER(fm.feature_type))=fo.cohort_type
  AND fm.os=fo.os
  AND fm.country=fo.country
WHERE
  feature_type IN ('Feature',
    'Vertical')
  AND feature_name <> 'Others'
  AND submission_date >= DATE '{start_date}'
  AND fo.cohort_type IS NULL
GROUP BY
  1,
  2,
  3,
  4,
  5
