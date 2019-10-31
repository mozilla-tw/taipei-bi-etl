DELETE `{project}.{dataset}.{dest}`
WHERE cohort_date >= DATE '{start_date}'
  AND cohort_date < DATE_ADD(DATE '{start_date}', INTERVAL '1' DAY)
