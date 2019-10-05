DELETE `{project}.{dataset}.{dest}` WHERE source='bukalapak'
  AND DATE(utc_datetime) >= DATE '{start_date}'
  AND DATE(utc_datetime) < DATE_ADD(DATE '{start_date}', INTERVAL '1' DAY)
