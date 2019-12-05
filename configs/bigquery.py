
BQ_PROJECT = {
    "project": "taipei-bi",
    "dataset": "mango_prod",
    "location": "US",
}

MANGO_CORE = {
    "type": "table",
    "partition_field": "submission_date",
    "allow_field_addition": True,
    "append": True,
    "params": {
        **BQ_PROJECT,
        # "src": "rocket-dev01.mango_dev.mango_core",
        "src": "moz-fx-data-shared-prod.telemetry.telemetry_core_parquet",
        "dest": "mango_core",
    },
    "udf": ["map_channels", "map_markets", "match_target_markets"],
    "query": "mango_core",
    "init_query": "init_mango_core",
    "cleanup_query": "cleanup_mango_core",
}

MANGO_CORE_NORMALIZED = {
    "type": "view",
    "params": {
        **BQ_PROJECT,
        "src": "mango_core",
        "dest": "mango_core_normalized",
    },
    "query": "mango_core_normalized",
}

MANGO_EVENTS = {
    "type": "table",
    "partition_field": "submission_date",
    "allow_field_addition": True,
    "append": True,
    "params": {
        **BQ_PROJECT,
        # "src": "rocket-dev01.mango_dev.mango_events",
        "src": "moz-fx-data-shared-prod.telemetry.focus_event",
        "dest": "mango_events",
    },
    "query": "mango_events",
    "init_query": "init_mango_events",
    "cleanup_query": "cleanup_mango_events",
}

MANGO_EVENTS_UNNESTED = {
    "type": "view",
    "params": {
        **BQ_PROJECT,
        "src": "mango_events",
        "dest": "mango_events_unnested",
    },
    "udf_js": ["cleanup_extra", "cleanup_settings"],
    "query": "mango_events_unnested",
}

MANGO_EVENTS_FEATURE_MAPPING = {
    "type": "view",
    "params": {
        **BQ_PROJECT,
        "src": "mango_events_unnested",
        "dest": "mango_events_feature_mapping",
    },
    "udf_js": ["feature_mapping"],
    "query": "mango_events_feature_mapping",
}

MANGO_CHANNEL_MAPPING = {
    "type": "gcs",
    "append": False,
    "latest_only": True,
    "filetype": "jsonl",
    "params": {
        **BQ_PROJECT,
        "src": "moz-taipei-bi/mango/staging-adjust-adjust_trackers/{start_date}.jsonl",
        "dest": "mango_channel_mapping",
    },
}

MANGO_USER_CHANNELS = {
    "type": "view",
    "params": {
        **BQ_PROJECT,
        "src": "mango_events",
        "src2": "mango_channel_mapping",
        "dest": "mango_user_channels",
    },
    "query": "mango_user_channels",
}

MANGO_USER_RFE_PARTIAL = {
    "type": "view",
    "params": {
        **BQ_PROJECT,
        "src": "mango_core_normalized",
        "src2": "mango_events_feature_mapping",
        "dest": "mango_user_rfe_daily_partial",
    },
    "query": "mango_user_rfe_daily_partial",
}

MANGO_USER_RFE_SESSION = {
    "type": "table",
    "partition_field": "submission_date",
    "append": True,
    "params": {
        **BQ_PROJECT,
        "execution_date_field": "submission_date",
        "src": "mango_events_feature_mapping",
        "src2": "mango_core",
        "dest": "mango_user_rfe_daily_session",
    },
    "query": "mango_user_rfe_daily_session",
}

MANGO_USER_RFE = {
    "type": "table",
    "allow_field_addition": True,
    "partition_field": "execution_date",
    "append": True,
    "params": {
        **BQ_PROJECT,
        # "src": "mango_core_normalized",
        "src": "mango_events",
        "src2": "mango_user_rfe_daily_partial",
        "src3": "mango_user_rfe_daily_session",
        "src4": "mango_user_channels",
        "dest": "mango_user_rfe_28d",
    },
    "query": "mango_user_rfe_28d",
    "cleanup_query": "cleanup_mango_user_rfe_28d",
}

MANGO_FEATURE_COHORT_DATE = {
    "type": "table",
    "partition_field": "cohort_date",
    "append": True,
    "params": {
        **BQ_PROJECT,
        "src": "mango_events_feature_mapping",
        "dest": "mango_feature_cohort_date",
    },
    "init_query": "init_mango_feature_cohort_date",
    "query": "mango_feature_cohort_date",
    "cleanup_query": "cleanup_mango_feature_cohort_date",
}

MANGO_USER_OCCURRENCE = {
    "type": "view",
    "params": {
        **BQ_PROJECT,
        "src": "mango_core_normalized",
        "src2": "mango_channel_mapping",
        "dest": "mango_user_occurrence",
    },
    "query": "mango_user_occurrence",
}

MANGO_USER_FEATURE_OCCURRENCE = {
    "type": "view",
    "params": {
        **BQ_PROJECT,
        "src": "mango_events_feature_mapping",
        "src2": "mango_feature_cohort_date",
        "dest": "mango_user_feature_occurrence",
    },
    "query": "mango_user_feature_occurrence",
}

MANGO_COHORT_USER_OCCURRENCE = {
    "type": "view",
    "params": {
        **BQ_PROJECT,
        "src": "mango_user_occurrence",
        "src2": "mango_user_channels",
        "src3": "mango_user_feature_occurrence",
        "dest": "mango_cohort_user_occurrence",
    },
    "query": "mango_cohort_user_occurrence",
}

MANGO_COHORT_RETAINED_USERS = {
    "type": "table",
    "partition_field": "cohort_date",
    # "backfill_days": [1, 7, 14, 21, 28, 35, 56, 63, 84, 91, 112],
    "append": True,
    "params": {
        **BQ_PROJECT,
        "src": "mango_cohort_user_occurrence",
        "dest": "mango_cohort_retained_users",
    },
    "query": "mango_cohort_retained_users",
    "init_query": "init_mango_cohort_retained_users",
    "cleanup_query": "cleanup_mango_cohort_retained_users",
}


MANGO_FEATURE_ACTIVE_NEW_USER_COUNT = {
    "type": "table",
    "allow_field_addition": True,
    "partition_field": "occur_date",
    "append": True,
    "params": {
        **BQ_PROJECT,
        "execution_date_field": "occur_date",
        "src": "mango_cohort_user_occurrence",
        "dest": "mango_feature_active_new_user_count",
    },
    "query": "mango_feature_active_new_user_count",
}

MANGO_FEATURE_ACTIVE_USER_COUNT = {
    "type": "table",
    "allow_field_addition": True,
    "partition_field": "submission_date",
    "append": True,
    "params": {
        **BQ_PROJECT,
        "execution_date_field": "submission_date",
        "src": "mango_user_rfe_daily_partial",
        "dest": "mango_feature_active_user_count",
    },
    "query": "mango_feature_active_user_count",
}

MANGO_FEATURE_ROI = {
    "type": "table",
    "allow_field_addition": True,
    "partition_field": "execution_date",
    "append": True,
    "params": {
        **BQ_PROJECT,
        "execution_date_field": "execution_date",
        "src": "mango_user_rfe_28d",
        "src2": "mango_cohort_retained_users",
        "src3": "mango_feature_active_user_count",
        "src4": "mango_feature_active_new_user_count",
        "dest": "mango_feature_roi",
    },
    "query": "mango_feature_roi",
}

GOOGLE_RPS = {
    "type": "gcs",
    "append": False,
    "filetype": "csv",
    "params": {
        **BQ_PROJECT,
        "src": "moz-taipei-bi/mango/staging-rps-google_search_rps/2018-01-01.csv",
        "dest": "google_rps",
    },
}

MANGO_REVENUE_BUKALAPAK = {
    "type": "gcs",
    "append": True,
    "filetype": "jsonl",
    "backfill_days": [1, 2, 3, 4, 5, 6, 7],
    "params": {
        **BQ_PROJECT,
        "src": "moz-taipei-bi/mango/staging-revenue-bukalapak/{start_date}.jsonl",
        "dest": "mango_revenue",
    },
    "cleanup_query": "cleanup_revenue_bukalapak",
}
