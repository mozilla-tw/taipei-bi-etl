with
core as (
  select distinct
    client_id,
    normalized_country as country,
    DATE_FROM_UNIX_DATE(normalized_profile_date) as profile_date
  from `{project}.{dataset}.{src}`
),

rfe_partial as (
  select
    client_id,
    os,
    country,
    submission_date,
    feature_type,
    feature_name,
    count(distinct concat(cast(submission_date as string), cast(event_timestamp as string))) as value_event_count -- 功能使用 n times, data type=int, 單位=int

  from `{project}.{dataset}.{src2}`
  where feature_name not in ('Others','feature: others')
  --and submission_date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)    --param
  group by
    client_id,
    os,
    country,
    submission_date,
    feature_type,
    feature_name
)

select

    -- ********** user header, by day, by feature
    rfe_partial.client_id,
    os,
    rfe_partial.country,
    profile_date,
    DATE_DIFF(submission_date, profile_date, DAY) as age, --submission_date - profile_date, int, 單位=days
    submission_date,
    feature_type,
    feature_name,

    --********** user engagement metrics
    value_event_count -- 功能使用 n times, data type=int, 單位=int

from rfe_partial
left join core on rfe_partial.client_id = core.client_id
and rfe_partial.country = core.country

