with 
active_days as (
  select 
    client_id, 
    count(distinct submission_date) as active_days --during 28d 出現幾天, int, 單位=days
  from `{project}.{dataset}.{src}`
  --where submission_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) -- 取 partition
  --and submission_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) 
  where submission_date > DATE_SUB(DATE('{start_date}'), INTERVAL 28 DAY) -- 取 partition
  and submission_date <= DATE('{start_date}')

  group by client_id
),

rfe_partial as (
  select

    -- ********** user header
    client_id,
    os,
    country,

    profile_date,
    DATE_DIFF(DATE('{start_date}'), profile_date, DAY) as age, --submission_date - profile_date, int, 單位=days  --param
    feature_type,
    feature_name,

    --********** user rfe metrics
    DATE_DIFF(DATE('{start_date}'), max(submission_date), DAY) as recency, --execution date - last seen, data type=int, 單位=days  --param
    count(distinct submission_date) as frequency_days, --during 28d 功能使用 n days, data type=int, 單位=days
    sum(value_event_count) as value_event_count --during 28d 功能使用 n times, data type=int, 單位=int

  from `{project}.{dataset}.{src2}`
  --where submission_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) --param
  --and submission_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) --param
  where submission_date > DATE_SUB(DATE('{start_date}'), INTERVAL 28 DAY) -- 取 partition
  and submission_date <= DATE('{start_date}')
  group by
    client_id,
    os,
    country,
    profile_date,
    feature_type,
    feature_name
),

rfe_session as (
  select

    -- ********** user header
    client_id,
    country,

    event_vertical,
    feature_type,
    feature_name,

    --********** user engagement metrics
    sum(session_time) as session_time,
    sum(url_counts) as url_counts,
    sum(app_link_install) as app_link_install,
    sum(app_link_open) as app_link_open,
    sum(show_keyboard) as show_keyboard

  from `{project}.{dataset}.{src3}`
  --where submission_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 29 DAY) -- 取 partition
  --and submission_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) 
  where submission_date > DATE_SUB(DATE('{start_date}'), INTERVAL 28 DAY) -- 取 partition
  and submission_date <= DATE('{start_date}')
  group by
    client_id,
    country,
    event_vertical,
    feature_type,
    feature_name
)


select

  -- ********** user header
  p.client_id,
  uc.network_name,
  p.os,
  p.country,
  p.profile_date,
  p.age,
  active_days.active_days, --during 28d 出現幾天, int, 單位=days
  p.feature_type,
  p.feature_name,

  -- ********** user rfe metrics
  recency, --execution date - last seen, data type=int, 單位=days
  frequency_days, --during 28d 功能使用 n days, data type=int, 單位=days
  value_event_count/frequency_days as value_event_count, --sum(value_event_count) / frequency, data type=float, 單位=次 per 使用日,
  session_time/frequency_days as session_time, --sum(session time) / frequency, data type=float, 單位=ms per 使用日
  url_counts/frequency_days as url_counts, --sum(url count) / frequency, data type=float, 單位=次 per 使用日
  app_link_install/frequency_days as app_link_install, --sum(partner app install count) / frequency, data type=float, 單位=次 per 使用日
  app_link_open/frequency_days as app_link_open, --sum(partner app launch count) / frequency, data type=float, 單位=次 per 使用日
  show_keyboard/frequency_days as show_keyboard, --sum(show keyboard count) / frequency, data type=float, 單位=次 per 使用日

  -- ********** execution date
  DATE('{start_date}') as execution_date -- 今天跑昨天以前的資料

from rfe_partial as p

left join active_days
  on p.client_id = active_days.client_id

left join rfe_session as s
  on p.client_id = s.client_id
  and p.feature_type = s.feature_type
  and p.feature_name = s.feature_name

left join `rocket-dev01.mango_dev3.mango_user_channels` AS uc
on p.client_id = uc.client_id
