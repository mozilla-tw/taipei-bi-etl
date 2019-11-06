SELECT
    submission_date_s3,
    submission_date,
    app_name,
    os,
    metadata,
    application,
    v,
    client_id,
    seq,
    locale,
    osversion,
    device,
    arch,
    profile_date,
    default_search,
    distribution_id,
    created,
    tz,
    sessions,
    durations,
    searches,
    experiments,
    flash_usage,
    campaign,
    campaign_id,
    default_browser,
    show_tracker_stats_share,
    accessibility_services,
    metadata_app_version,
    bug_1501329_affected
FROM `{src}` WHERE app_name='Zerda'
  AND submission_date >= DATE '2018-11-01'
  AND submission_date <= DATE '{start_date}'
