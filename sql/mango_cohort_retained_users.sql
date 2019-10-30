WITH
  daily_retained AS (
  SELECT
    os,
    country,
    measure_type,
    cohort_level,
    cohort_name,
    cohort_date,
    occur_day,
    COUNT(DISTINCT client_id) AS users
  FROM
    `rocket-dev01.mango_dev.cohort_user_occurrence`
  WHERE
    occur_day IN (0,
      1,
      7,
      14,
      28,
      56,
      84)
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7),
  weekly_retained AS (
  SELECT
    os,
    country,
    measure_type,
    cohort_level,
    cohort_name,
    cohort_date,
    occur_week,
    COUNT(DISTINCT client_id) AS users
  FROM
    `rocket-dev01.mango_dev.cohort_user_occurrence`
  WHERE
    occur_week IN (0,
      1,
      2,
      3,
      4,
      8,
      12)
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7),
  monthly_retained AS (
  SELECT
    os,
    country,
    measure_type,
    cohort_level,
    cohort_name,
    cohort_date,
    occur_month,
    COUNT(DISTINCT client_id) AS users
  FROM
    `rocket-dev01.mango_dev.cohort_user_occurrence`
  WHERE
    occur_month IN (0,
      1,
      2,
      3)
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7),
  all_retained AS (
  SELECT
    os,
    country,
    measure_type,
    cohort_level,
    cohort_name,
    cohort_date,
    'day' AS occur_type,
    occur_day AS occur_num,
    users
  FROM
    daily_retained
  UNION ALL
  SELECT
    os,
    country,
    measure_type,
    cohort_level,
    cohort_name,
    cohort_date,
    'week' AS occur_type,
    occur_week AS occur_num,
    users
  FROM
    weekly_retained
  UNION ALL
  SELECT
    os,
    country,
    measure_type,
    cohort_level,
    cohort_name,
    cohort_date,
    'month' AS occur_type,
    occur_month AS occur_num,
    users
  FROM
    monthly_retained ),
  all_retained_pivot AS (
  SELECT
    os,
    country,
    measure_type,
    cohort_level,
    cohort_name,
    cohort_date,
    MAX(
    IF
      (occur_type = 'day'
        AND occur_num = 0,
        users,
        NULL)) AS daily_cohort_size,
    MAX(
    IF
      (occur_type = 'day'
        AND occur_num = 1,
        users,
        0)) AS d1_retained_users,
    MAX(
    IF
      (occur_type = 'day'
        AND occur_num = 7,
        users,
        0)) AS d7_retained_users,
    MAX(
    IF
      (occur_type = 'day'
        AND occur_num = 14,
        users,
        0)) AS d14_retained_users,
    MAX(
    IF
      (occur_type = 'day'
        AND occur_num = 28,
        users,
        0)) AS d28_retained_users,
    MAX(
    IF
      (occur_type = 'day'
        AND occur_num = 56,
        users,
        0)) AS d56_retained_users,
    MAX(
    IF
      (occur_type = 'day'
        AND occur_num = 84,
        users,
        0)) AS d84_retained_users,
    MAX(
    IF
      (occur_type = 'week'
        AND occur_num = 0,
        users,
        0)) AS weekly_cohort_size,
    MAX(
    IF
      (occur_type = 'week'
        AND occur_num = 1,
        users,
        0)) AS w1_retained_users,
    MAX(
    IF
      (occur_type = 'week'
        AND occur_num = 2,
        users,
        0)) AS w2_retained_users,
    MAX(
    IF
      (occur_type = 'week'
        AND occur_num = 4,
        users,
        0)) AS w4_retained_users,
    MAX(
    IF
      (occur_type = 'week'
        AND occur_num = 8,
        users,
        0)) AS w8_retained_users,
    MAX(
    IF
      (occur_type = 'week'
        AND occur_num = 12,
        users,
        0)) AS w12_retained_users,
    MAX(
    IF
      (occur_type = 'month'
        AND occur_num = 0,
        users,
        0)) AS monthly_cohort_size,
    MAX(
    IF
      (occur_type = 'month'
        AND occur_num = 1,
        users,
        0)) AS m1_retained_users,
    MAX(
    IF
      (occur_type = 'month'
        AND occur_num = 2,
        users,
        0)) AS m2_retained_users,
    MAX(
    IF
      (occur_type = 'month'
        AND occur_num = 3,
        users,
        0)) AS m3_retained_users
  FROM
    all_retained
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6)
SELECT
  *
FROM
  all_retained_pivot
WHERE
  daily_cohort_size > 0
ORDER BY
  1,
  2,
  3,
  4,
  5,
  6