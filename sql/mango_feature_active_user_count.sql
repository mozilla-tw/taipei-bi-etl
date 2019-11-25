-- to be deprecated
with
dau as (
  SELECT
    os,
    country,
    feature_type AS cohort_level,
    feature_name AS cohort_name,
    submission_date,
    COUNT(DISTINCT client_id) AS dau
  FROM
    `{project}.{dataset}.{src}`
  WHERE
    submission_date = DATE('{start_date}')
  GROUP BY
    os,
    country,
    feature_type,
    feature_name,
    submission_date
),

wau as (
  SELECT
    os,
    country,
    feature_type AS cohort_level,
    feature_name AS cohort_name,
    count(distinct client_id) AS wau
  FROM
    `{project}.{dataset}.{src}`
  WHERE
    submission_date >= DATE_SUB(DATE('{start_date}'), INTERVAL 6 DAY) -- 取 partition
    AND submission_date <= DATE('{start_date}')
  group by
    os,
    country,
    feature_type,
    feature_name
),

mau as (
  SELECT
    os,
    country,
    feature_type AS cohort_level,
    feature_name AS cohort_name,
    count(distinct client_id) AS mau
  FROM
    `{project}.{dataset}.{src}`
  WHERE
    submission_date >= DATE_SUB(DATE('{start_date}'), INTERVAL 27 DAY) -- 取 partition
    AND submission_date <= DATE('{start_date}')
  group by
    os,
    country,
    feature_type,
    feature_name
)


SELECT dau.*,
      wau.wau,
      mau.mau
FROM dau

left join wau
on dau.os = wau.os
and  dau.country = wau.country
and  dau.cohort_level = wau.cohort_level
and  dau.cohort_name = wau.cohort_name

left join mau
on dau.os = mau.os
and  dau.country = mau.country
and  dau.cohort_level = mau.cohort_level
and  dau.cohort_name = mau.cohort_name
