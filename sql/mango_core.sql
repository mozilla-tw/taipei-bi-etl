SELECT * FROM `{src}` WHERE app_name='Zerda'
  AND submission_date >= DATE '{start_date}'
  AND submission_date < DATE_ADD(DATE '{start_date}', INTERVAL '1' DAY)
