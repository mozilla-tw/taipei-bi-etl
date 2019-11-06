DELETE `{project}.{dataset}.{dest}`
WHERE cohort_date >= DATE_SUB(DATE '{start_date}', INTERVAL 112 DAY)
-- wipe out data starting from earliest backfill date