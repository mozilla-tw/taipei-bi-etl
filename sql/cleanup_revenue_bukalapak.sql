DELETE `{project}.{dataset}.{dest}` WHERE source='bukalapak'
  AND DATE(utc_datetime) = DATE '{start_date}'
