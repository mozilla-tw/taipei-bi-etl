DELETE `{project}.{dataset}.{dest}`
WHERE cohort_date >= DATE '{start_date}'
-- wipe out data starting from earliest backfill date