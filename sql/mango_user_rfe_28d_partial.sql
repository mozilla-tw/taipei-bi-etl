with core as (
  select distinct
    client_id,
    normalized_country as country,
    DATE_FROM_UNIX_DATE(normalized_profile_date) as profile_date
  from `{project}.{dataset}.{src}`
),

feature as (
  select
    *,
    DATE('{start_date}') as execution_date -- 今天跑昨天以前的資料
  from `{project}.{dataset}.{src2}`
  where submission_date >= DATE_SUB(DATE('{start_date}'), INTERVAL 27 DAY) -- 取 partition
  and submission_date < DATE_ADD(DATE('{start_date}'), INTERVAL 1 DAY)
  and feature_name not in ('Others','feature: others')
),

active_days as (
  select
    client_id,
    count(distinct submission_date) as active_days --during 28d 出現幾天, int, 單位=days
  from feature
  group by client_id
),

rfe as (
  select
    client_id, os, country,
    feature_type, feature_name,
    execution_date,
    DATE_DIFF(execution_date, max(submission_date), DAY) as recency, --execution date - last seen, data type=int, 單位=days
    count(distinct submission_date) as frequency_days, --during 28d 功能使用 n days, data type=int, 單位=days
    count(distinct concat(cast(submission_date as string), cast(event_timestamp as string))) as value_event_count --during 28d 功能使用 n times, data type=int, 單位=int
  from feature
  group by client_id, os, country,
           feature_type, feature_name,
           execution_date
)

select
    rfe.client_id,
    os,
    rfe.country,
    profile_date,
    DATE_DIFF(execution_date, profile_date, DAY) as age, --execution date - profile_date, int, 單位=days
    active_days.active_days,
    feature_type, feature_name,
    recency,
    frequency_days,
    value_event_count,
    execution_date
from rfe
left join core on rfe.client_id = core.client_id and rfe.country = core.country
left join active_days on rfe.client_id = active_days.client_id
