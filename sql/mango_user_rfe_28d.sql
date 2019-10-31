select

  -- ********** user header
  p.client_id,
  p.os,
  p.country,
  p.profile_date,
  p.age,
  p.active_days, --during 28d 出現幾天, int, 單位=days
  p.feature_type,
  p.feature_name,

  -- ********** user rfe metrics
  recency, --execution date - last seen, data type=int, 單位=days
  frequency_days, --during 28d 功能使用 n days, data type=int, 單位=days
  value_event_count/frequency_days as value_event_count, --sum(value_event_count time) / frequency, data type=float, 單位=float per 使用日,
  session_time/frequency_days as session_time, --sum(session time) / frequency, data type=float, 單位=ms per 使用日
  url_counts/frequency_days as url_counts, --sum(url count) / frequency, data type=float, 單位=次 per 使用日
  app_link_install/frequency_days as app_link_install, --sum(partner app install count) / frequency, data type=float, 單位=次 per 使用日
  app_link_open/frequency_days as app_link_open, --sum(partner app launch count) / frequency, data type=float, 單位=次 per 使用日
  show_keyboard/frequency_days as show_keyboard, --sum(show keyboard count) / frequency, data type=float, 單位=次 per 使用日

  -- ********** execution date
  p.execution_date

from `{project}.{dataset}.{src}` as p
left join `{project}.{dataset}.{src2}` as s
  on p.client_id = s.client_id
  and p.feature_type = s.feature_type
  and p.feature_name = s.feature_name
  and p.execution_date = s.execution_date