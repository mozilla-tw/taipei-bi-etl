DELETE `{project}.{dataset}.{dest}`
WHERE execution_date >= DATE '{start_date}'
  AND execution_date < DATE_ADD(DATE '{start_date}', INTERVAL '1' DAY)
