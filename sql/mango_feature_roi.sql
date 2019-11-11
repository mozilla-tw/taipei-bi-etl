with
rfe as (
  SELECT distinct
    os,
    country,
    feature_type,
    feature_name,

    --active_days: during 28d FF Lite 出現幾天, int, 單位=days
    PERCENTILE_CONT(active_days, 0.25) OVER(partition by os, country, feature_type, feature_name) AS active_days_25p,
    PERCENTILE_CONT(active_days, 0.5) OVER(partition by os, country, feature_type, feature_name) AS active_days_50p,
    PERCENTILE_CONT(active_days, 0.75) OVER(partition by os, country, feature_type, feature_name) AS active_days_75p,

    --recency: execution date - last seen, data type=int, 單位=days
    PERCENTILE_CONT(recency, 0.25) OVER(partition by os, country, feature_type, feature_name) AS recency_25p,
    PERCENTILE_CONT(recency, 0.5) OVER(partition by os, country, feature_type, feature_name) AS recency_50p,
    PERCENTILE_CONT(recency, 0.75) OVER(partition by os, country, feature_type, feature_name) AS recency_75p,

    --frequency_days: during 28d 功能使用 n days, data type=int, 單位=days
    PERCENTILE_CONT(frequency_days, 0.25) OVER(partition by os, country, feature_type, feature_name) AS frequency_days_25p,
    PERCENTILE_CONT(frequency_days, 0.5) OVER(partition by os, country, feature_type, feature_name) AS frequency_days_50p,
    PERCENTILE_CONT(frequency_days, 0.75) OVER(partition by os, country, feature_type, feature_name) AS frequency_days_75p,

    --value_event_count: sum(value_event_count) / frequency, data type=float, 單位=次 per 使用日
    PERCENTILE_CONT(value_event_count, 0.25) OVER(partition by os, country, feature_type, feature_name) AS value_event_count_25p,
    PERCENTILE_CONT(value_event_count, 0.5) OVER(partition by os, country, feature_type, feature_name) AS value_event_count_50p,
    PERCENTILE_CONT(value_event_count, 0.75) OVER(partition by os, country, feature_type, feature_name) AS value_event_count_75p,

    --session_time: sum(session time) / frequency, data type=float, 單位=ms per 使用日
    PERCENTILE_CONT(session_time, 0.25) OVER(partition by os, country, feature_type, feature_name) AS session_time_25p,
    PERCENTILE_CONT(session_time, 0.5) OVER(partition by os, country, feature_type, feature_name) AS session_time_50p,
    PERCENTILE_CONT(session_time, 0.75) OVER(partition by os, country, feature_type, feature_name) AS session_time_75p,

    --url_counts: sum(url count) / frequency, data type=float, 單位=次 per 使用日
    PERCENTILE_CONT(url_counts, 0.25) OVER(partition by os, country, feature_type, feature_name) AS url_counts_25p,
    PERCENTILE_CONT(url_counts, 0.5) OVER(partition by os, country, feature_type, feature_name) AS url_counts_50p,
    PERCENTILE_CONT(url_counts, 0.75) OVER(partition by os, country, feature_type, feature_name) AS url_counts_75p,

    --app_link_install: sum(partner app install count) / frequency, data type=float, 單位=次 per 使用日
    PERCENTILE_CONT(app_link_install, 0.25) OVER(partition by os, country, feature_type, feature_name) AS app_link_install_25p,
    PERCENTILE_CONT(app_link_install, 0.5) OVER(partition by os, country, feature_type, feature_name) AS app_link_install_50p,
    PERCENTILE_CONT(app_link_install, 0.75) OVER(partition by os, country, feature_type, feature_name) AS app_link_install_75p,

    --app_link_open: sum(partner app launch count) / frequency, data type=float, 單位=次 per 使用日
    PERCENTILE_CONT(app_link_open, 0.25) OVER(partition by os, country, feature_type, feature_name) AS app_link_open_25p,
    PERCENTILE_CONT(app_link_open, 0.5) OVER(partition by os, country, feature_type, feature_name) AS app_link_open_50p,
    PERCENTILE_CONT(app_link_open, 0.75) OVER(partition by os, country, feature_type, feature_name) AS app_link_open_75p,

    --show_keyboard: sum(show keyboard count) / frequency, data type=float, 單位=次 per 使用日
    PERCENTILE_CONT(show_keyboard, 0.25) OVER(partition by os, country, feature_type, feature_name) AS show_keyboard_25p,
    PERCENTILE_CONT(show_keyboard, 0.5) OVER(partition by os, country, feature_type, feature_name) AS show_keyboard_50p,
    PERCENTILE_CONT(show_keyboard, 0.75) OVER(partition by os, country, feature_type, feature_name) AS show_keyboard_75p,
    execution_date

  FROM `{project}.{dataset}.{src}`
  WHERE execution_date = DATE '{start_date}'
),

rr as (
  SELECT
    os,
    country,
    cohort_level,
    cohort_name,

    sum(d1_retained_users)/sum(daily_cohort_size) AS d1_retention,
    sum(d7_retained_users)/sum(daily_cohort_size) AS d7_retention,
    sum(d14_retained_users)/sum(daily_cohort_size) AS d14_retention,
    sum(d28_retained_users)/sum(daily_cohort_size) AS d28_retention,
    sum(d56_retained_users)/sum(daily_cohort_size) AS d56_retention,
    sum(d84_retained_users)/sum(daily_cohort_size) AS d84_retention,

    sum(w1_retained_users)/sum(weekly_cohort_size) AS w1_retention,
    sum(w2_retained_users)/sum(weekly_cohort_size) AS w2_retention,
    sum(w4_retained_users)/sum(weekly_cohort_size) AS w4_retention,
    sum(w8_retained_users)/sum(weekly_cohort_size) AS w8_retention,
    sum(w12_retained_users)/sum(weekly_cohort_size) AS w12_retention,

    sum(m1_retained_users)/sum(monthly_cohort_size) AS m1_retention,
    sum(m2_retained_users)/sum(monthly_cohort_size) AS m2_retention,
    sum(m3_retained_users)/sum(monthly_cohort_size) AS m3_retention

  FROM
    `{project}.{dataset}.{src2}`
  where measure_type = 'feature'
  and cohort_date > DATE_SUB(DATE '{start_date}', INTERVAL 28 DAY)
  and cohort_date <= DATE '{start_date}'

  group by
    os,
    country,
    cohort_level,
    cohort_name
),

au as (
  select
    os,
    country,
    cohort_level,
    cohort_name,
    avg(dau) as dau,
    avg(wau) as wau,
    avg(mau) as mau
  from `{project}.{dataset}.{src3}`
  where submission_date > DATE_SUB(DATE '{start_date}', INTERVAL 28 DAY)
  and submission_date <= DATE '{start_date}'
  group by
    os,
    country,
    cohort_level,
    cohort_name
),

new_au as (
  select
    os,
    country,
    cohort_level,
    cohort_name,
    avg(dau) as dau,
    avg(wau) as wau,
    avg(mau) as mau
  from `{project}.{dataset}.{src4}`
  where occur_date > DATE_SUB(DATE '{start_date}', INTERVAL 28 DAY)
  and occur_date <= DATE '{start_date}'
  group by
    os,
    country,
    cohort_level,
    cohort_name
)

select
    rfe.os,
    rfe.country,
    rfe.feature_type as cohort_level,
    rfe.feature_name as cohort_name,

    --active_users: during 28d FF Lite 多少不重複使用者, int, 單位=人
    au.dau as aDAU,
    au.wau as aWAU,
    au.mau as aMAU,
    new_au.dau as new_aDAU,
    new_au.wau as new_aWAU,
    new_au.mau as new_aMAU,

    --active_days: during 28d FF Lite 出現幾天, int, 單位=days
    active_days_25p,
    active_days_50p,
    active_days_75p,

    --recency: execution date - last seen, data type=int, 單位=days
    recency_25p,
    recency_50p,
    recency_75p,

    --frequency_days: during 28d 功能使用 n days, data type=int, 單位=days
    frequency_days_25p,
    frequency_days_50p,
    frequency_days_75p,

    --value_event_count: sum(value_event_count) / frequency, data type=float, 單位=次 per 使用日
    value_event_count_25p,
    value_event_count_50p,
    value_event_count_75p,

    --session_time: sum(session time) / frequency, data type=float, 單位=ms per 使用日
    session_time_25p,
    session_time_50p,
    session_time_75p,

    --url_counts: sum(url count) / frequency, data type=float, 單位=次 per 使用日
    url_counts_25p,
    url_counts_50p,
    url_counts_75p,

    --app_link_install: sum(partner app install count) / frequency, data type=float, 單位=次 per 使用日
    app_link_install_25p,
    app_link_install_50p,
    app_link_install_75p,

    --app_link_open: sum(partner app launch count) / frequency, data type=float, 單位=次 per 使用日
    app_link_open_25p,
    app_link_open_50p,
    app_link_open_75p,

    --show_keyboard: sum(show keyboard count) / frequency, data type=float, 單位=次 per 使用日
    show_keyboard_25p,
    show_keyboard_50p,
    show_keyboard_75p,
    rfe.execution_date

from rfe
left join rr
on rfe.os = rr.os
and rfe.country = rr.country
and rfe.feature_type = rr.cohort_level
and rfe.feature_name = rr.cohort_name

left join au
on rfe.os = au.os
and rfe.country = au.country
and rfe.feature_type = au.cohort_level
and rfe.feature_name = au.cohort_name

left join new_au
on rfe.os = new_au.os
and rfe.country = new_au.country
and rfe.feature_type = new_au.cohort_level
and rfe.feature_name = new_au.cohort_name

