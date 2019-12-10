WITH
  cohort AS (
  SELECT
    'feature' AS measure_type,
    fm.feature_type AS cohort_level,
    fm.feature_name AS cohort_name,
    fm.os,
    fm.country,
    fm.client_id,
    fo.cohort_date,
    fm.submission_date
  FROM
    `{project}.{dataset}.{src}` AS fm
  LEFT JOIN
    `{project}.{dataset}.{src2}` AS fo
  ON
    fm.client_id=fo.client_id
    AND fm.country=fo.country
    AND fm.os=fo.os
    AND fm.feature_type=fo.cohort_level
    AND fm.feature_name=fo.cohort_name
  WHERE
    fm.country IS NOT NULL
    AND feature_name not in ('Others','feature: others')
  ),
  occur AS (
  SELECT
    measure_type,
    cohort_level,
    cohort_name,
    os,
    country,
    client_id,
    cohort_date,
    submission_date AS occur_date,
    DATE_DIFF(submission_date, cohort_date, DAY) AS occur_day
  FROM
    cohort
  WHERE
    cohort_date IS NOT NULL
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8
--   HAVING
--     occur_day < 112
--     AND occur_day >= 0
  ),
  occur_day_week_month AS (
  SELECT
    *,
    CAST(FLOOR(occur_day/7) AS INT64) AS occur_week,
    CAST(FLOOR(occur_day/28) AS INT64) AS occur_month
  FROM
    occur)
SELECT
  *
FROM
  occur_day_week_month