SELECT *,
CASE metadata.geo_country WHEN '??' THEN null ELSE metadata.geo_country END AS normalized_country,
CASE WHEN DATE_FROM_UNIX_DATE(profile_date) BETWEEN DATE('2017-10-01') AND CURRENT_DATE THEN null ELSE profile_date END AS normalized_profile_date
FROM `{project}.{dataset}.{src}`