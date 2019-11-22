DELETE `{project}.{dataset}.{dest}`
WHERE client_id IN (
  SELECT
    distinct client_id
  FROM
    `{project}.{dataset}.{src}`
  WHERE
    submission_date = DATE '{start_date}'
)
