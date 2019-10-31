WITH
  tracker_settings AS (
  SELECT
    client_id,
    settings.value AS settings_value,
    RANK() OVER(PARTITION BY client_id ORDER BY submission_timestamp DESC) AS rank
  FROM
    `{project}.{dataset}.{src}`
  LEFT JOIN
    UNNEST(settings) AS settings
  WHERE
    settings.key='pref_key_s_tracker_token'
    OR settings.key IS NULL ),
  user_token AS (
  SELECT
    client_id,
    settings_value
  FROM
    tracker_settings
  WHERE
    rank=1 ),
  user_channels AS (
  SELECT
    client_id,
    settings_value,
    channels.network_name,
    channels.network_token,
    channels.campaign_name,
    channels.campaign_token,
    channels.adgroup_name,
    channels.adgroup_token,
    channels.creative_name,
    channels.creative_token
  FROM
    user_token
  JOIN
    `rocket-dev01.mango_dev.channel_mapping` AS channels
  ON
    user_token.settings_value=channels.network_token
  UNION ALL
  SELECT
    client_id,
    settings_value,
    channels.network_name,
    channels.network_token,
    channels.campaign_name,
    channels.campaign_token,
    channels.adgroup_name,
    channels.adgroup_token,
    channels.creative_name,
    channels.creative_token
  FROM
    user_token
  JOIN
    `{project}.{dataset}.{src2}` AS channels
  ON
    user_token.settings_value=channels.campaign_token
  UNION ALL
  SELECT
    client_id,
    settings_value,
    channels.network_name,
    channels.network_token,
    channels.campaign_name,
    channels.campaign_token,
    channels.adgroup_name,
    channels.adgroup_token,
    channels.creative_name,
    channels.creative_token
  FROM
    user_token
  JOIN
    `{project}.{dataset}.{src2}` AS channels
  ON
    user_token.settings_value=channels.adgroup_token
  UNION ALL
  SELECT
    client_id,
    settings_value,
    channels.network_name,
    channels.network_token,
    channels.campaign_name,
    channels.campaign_token,
    channels.adgroup_name,
    channels.adgroup_token,
    channels.creative_name,
    channels.creative_token
  FROM
    user_token
  JOIN
    `{project}.{dataset}.{src2}` AS channels
  ON
    user_token.settings_value=channels.creative_token )
SELECT
  user_channels.client_id,
  user_channels.settings_value,
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
    '0') AS creative_token
FROM
  user_channels