with
feature_age as (
  SELECT
    os,
    country,
    cohort_level,
    cohort_name,
    client_id,
    occur_date
  FROM `{project}.{dataset}.{src}`
  where measure_type = 'feature'
  and occur_day = 0
),

dau as (
  SELECT
    os,
    country,
    cohort_level,
    cohort_name,
    occur_date,
    COUNT(DISTINCT client_id) AS dau
  FROM
    feature_age
  WHERE
    occur_date = DATE('{start_date}')
  GROUP BY
    os,
    country,
    cohort_level,
    cohort_name,
    occur_date
),

wau as (
  SELECT
    os,
    country,
    cohort_level,
    cohort_name,
    COUNT(DISTINCT client_id) AS wau
  FROM
    feature_age
  WHERE
    occur_date >= DATE_SUB(DATE('{start_date}'), INTERVAL 6 DAY) -- 取 partition
    AND occur_date <= DATE('{start_date}')
  GROUP BY
    os,
    country,
    cohort_level,
    cohort_name
),

mau as (
  SELECT
    os,
    country,
    cohort_level,
    cohort_name,
    COUNT(DISTINCT client_id) AS mau
  FROM
    feature_age
  WHERE
    occur_date >= DATE_SUB(DATE('{start_date}'), INTERVAL 27 DAY) -- 取 partition
    AND occur_date <= DATE('{start_date}')
  GROUP BY
    os,
    country,
    cohort_level,
    cohort_name
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
