SELECT *,
CASE metadata.geo_country WHEN '??' THEN null ELSE metadata.geo_country END AS normalized_country,
CASE WHEN DATE_ADD(DATE '1970-01-01', INTERVAL profile_date DAY) < DATE('2017-10-01') THEN null ELSE profile_date END AS normalized_profile_date
FROM `{src}` WHERE app_name='Zerda'
  AND submission_date >= DATE '{start_date}'
  AND submission_date < DATE_ADD(DATE '{start_date}', INTERVAL '1' DAY)
