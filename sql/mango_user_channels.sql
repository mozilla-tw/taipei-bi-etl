WITH
  tracker_settings AS (
  SELECT
    client_id,
    MAX((SELECT value FROM UNNEST(settings) WHERE key = 'pref_key_s_tracker_token')) AS tracker_token,
    MAX((SELECT value FROM UNNEST(settings) WHERE key = 'install_referrer')) AS install_referrer,
    MAX(submission_date) AS execution_date
  FROM
    `{project}.{dataset}.{src}`
  WHERE
    submission_date = DATE '{start_date}'
  GROUP BY 1
  ),
  user_channels AS (
  SELECT
    client_id,
    tracker_token,
    install_referrer,
    channels.network_name,
    channels.network_token,
    channels.campaign_name,
    channels.campaign_token,
    channels.adgroup_name,
    channels.adgroup_token,
    channels.creative_name,
    channels.creative_token,
    settings.execution_date
  FROM
    tracker_settings AS settings
  JOIN
    `{project}.{dataset}.{src2}` AS channels
  ON
    settings.tracker_token=channels.network_token
  UNION ALL
  SELECT
    client_id,
    tracker_token,
    install_referrer,
    channels.network_name,
    channels.network_token,
    channels.campaign_name,
    channels.campaign_token,
    channels.adgroup_name,
    channels.adgroup_token,
    channels.creative_name,
    channels.creative_token,
    settings.execution_date
  FROM
    tracker_settings AS settings
  JOIN
    `{project}.{dataset}.{src2}` AS channels
  ON
    settings.tracker_token=channels.campaign_token
  UNION ALL
  SELECT
    client_id,
    tracker_token,
    install_referrer,
    channels.network_name,
    channels.network_token,
    channels.campaign_name,
    channels.campaign_token,
    channels.adgroup_name,
    channels.adgroup_token,
    channels.creative_name,
    channels.creative_token,
    settings.execution_date
  FROM
    tracker_settings AS settings
  JOIN
    `{project}.{dataset}.{src2}` AS channels
  ON
    settings.tracker_token=channels.adgroup_token
  UNION ALL
  SELECT
    client_id,
    tracker_token,
    install_referrer,
    channels.network_name,
    channels.network_token,
    channels.campaign_name,
    channels.campaign_token,
    channels.adgroup_name,
    channels.adgroup_token,
    channels.creative_name,
    channels.creative_token,
    settings.execution_date
  FROM
    tracker_settings AS settings
  JOIN
    `{project}.{dataset}.{src2}` AS channels
  ON
    settings.tracker_token=channels.creative_token
  UNION ALL
  SELECT
    client_id,
    tracker_token,
    install_referrer,
    NULL AS network_name,
    NULL AS network_token,
    NULL AS campaign_name,
    NULL AS campaign_token,
    NULL AS adgroup_name,
    NULL AS adgroup_token,
    NULL AS creative_name,
    NULL AS creative_token,
    settings.execution_date
  FROM
    tracker_settings AS settings
  WHERE
    tracker_token IS NULL
),
rs AS (SELECT
  user_channels.client_id,
  user_channels.tracker_token,
  user_channels.install_referrer,
  IFNULL(user_channels.network_name,
    'unknown') AS network_name,
  IFNULL(user_channels.network_token,
    '0') AS network_token,
  IFNULL(user_channels.campaign_name,
    'unknown') AS campaign_name,
  IFNULL(user_channels.campaign_token,
    '0') AS campaign_token,
  IFNULL(user_channels.adgroup_name,
    'unknown') AS adgroup_name,
  IFNULL(user_channels.adgroup_token,
    '0') AS adgroup_token,
  IFNULL(user_channels.creative_name,
    'unknown') AS creative_name,
  IFNULL(user_channels.creative_token,
    '0') AS creative_token,
  RANK() OVER (PARTITION BY client_id ORDER BY creative_token ASC) AS r,  -- dedup creative level
  execution_date
FROM
  user_channels)
SELECT * FROM rs WHERE r=1