WITH
  cohort AS (
  SELECT
    os,
    normalized_country AS country,
    client_id,
    DATE_FROM_UNIX_DATE(normalized_profile_date) AS cohort_date,
    submission_date
  FROM
    `{project}.{dataset}.{src}`
  WHERE
    normalized_profile_date IS NOT NULL),
  occur AS (
  SELECT
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
    5
  HAVING
    occur_day < 112
    AND occur_day >= 0),
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