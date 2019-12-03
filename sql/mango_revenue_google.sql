WITH
  vol AS (
  SELECT
    submission_date AS date,
    metadata.geo_country AS country,
    t.key AS entrypoint,
    SUM(t.value) AS volume
  FROM
    `{project}.{dataset}.{src}`
  CROSS JOIN
    UNNEST(searches) AS t
  WHERE
    app_name = 'Zerda'
    AND os = 'Android'
    AND submission_date = DATE '{start_date}'
    AND t.value < 10000
    AND t.key like '%google%'
  GROUP BY
    1,
    2,
    3)
SELECT
  'estimated' AS conversion_status,
  'Android' AS os,
  vol.country,
  vol.date AS utc_date,
  DATETIME(vol.date) AS utc_datetime,
  '+00:00' AS tz,
  'google' AS source,
  CAST(vol.volume AS FLOAT64) AS sales_amount,
  vol.volume * rps.rps AS payout,
  'USD' AS currency,
  entrypoint AS fx_defined1,
  '' AS fx_defined2,
  '' AS fx_defined3,
  '' AS fx_defined4,
  '' AS fx_defined5
FROM
  vol
LEFT JOIN
  `{project}.{dataset}.{src2}` AS rps
ON
  vol.country=rps.country
