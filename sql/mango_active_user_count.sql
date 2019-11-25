with
occur as (
  SELECT
    os,
    country,
    measure_type,
    cohort_level,
    cohort_name,
    client_id,
    CASE WHEN occur_day = 0 THEN client_id ELSE null END AS new_client_id,
    occur_date
  FROM `{project}.{dataset}.{src}`
  where occur_date >= DATE_SUB(DATE('{start_date}'), INTERVAL 27 DAY) -- 取 partition
    AND occur_date <= DATE('{start_date}')
),

dau as (
  SELECT
    os,
    country,
    measure_type,
    cohort_level,
    cohort_name,
    occur_date,
    COUNT(DISTINCT new_client_id) AS new_dau,
    COUNT(DISTINCT client_id) AS dau
  FROM
    occur
  WHERE
    occur_date = DATE('{start_date}')
  GROUP BY
    os,
    country,
    measure_type,
    cohort_level,
    cohort_name,
    occur_date
),

wau as (
  SELECT
    os,
    country,
    measure_type,
    cohort_level,
    cohort_name,
    COUNT(DISTINCT new_client_id) AS new_wau,
    COUNT(DISTINCT client_id) AS wau
  FROM
    occur
  WHERE
--     measure_type = 'feature'
    occur_date >= DATE_SUB(DATE('{start_date}'), INTERVAL 6 DAY) -- 取 partition
    AND occur_date <= DATE('{start_date}')
  GROUP BY
    os,
    country,
    measure_type,
    cohort_level,
    cohort_name
),

mau as (
  SELECT
    os,
    country,
    measure_type,
    cohort_level,
    cohort_name,
    COUNT(DISTINCT new_client_id) AS new_mau,
    COUNT(DISTINCT client_id) AS mau
  FROM
    occur
  WHERE
--     measure_type = 'feature'
    occur_date >= DATE_SUB(DATE('{start_date}'), INTERVAL 27 DAY) -- 取 partition
    AND occur_date <= DATE('{start_date}')
  GROUP BY
    os,
    country,
    measure_type,
    cohort_level,
    cohort_name
)

SELECT dau.*,
      wau.new_wau,
      wau.wau,
      mau.new_mau,
      mau.mau
FROM dau

left join wau
on dau.os = wau.os
and  dau.country = wau.country
and  dau.cohort_level = wau.cohort_level
and  dau.cohort_name = wau.cohort_name
and  dau.measure_type = wau.measure_type

left join mau
on dau.os = mau.os
and  dau.country = mau.country
and  dau.cohort_level = mau.cohort_level
and  dau.cohort_name = mau.cohort_name
and  dau.measure_type = mau.measure_type
