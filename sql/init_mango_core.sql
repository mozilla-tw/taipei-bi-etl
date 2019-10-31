SELECT *
FROM `{src}` WHERE app_name='Zerda'
  AND submission_date >= DATE '2018-11-01'
  AND submission_date < DATE_ADD(DATE '{start_date}', INTERVAL '1' DAY)
