from google.cloud import bigquery

BQ_PROJECT = {"dataset": "test", "location": "US", "project": "rocket-dev01"}


MANGO_EVENTS = {
    "type": "table",
    "partition_field": "submission_date",
    "append": True,
    "params": {
        **BQ_PROJECT,
        "src": "rocket-dev01.unittest_assets.mango_events",
        "dest": "mango_events",
    },
    "query": "mango_events",
    "cleanup_query": "cleanup_mango_events",
}

MANGO_EVENTS_UNNESTED = {
    "type": "view",
    "params": {**BQ_PROJECT, "src": "mango_events", "dest": "mango_events_unnested"},
    "udf_js": ["json_extract_events"],
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
    "filetype": "jsonl",
    "days_behind": 0,
    "params": {
        **BQ_PROJECT,
        "src": "unittest-assets-rocket-dev01/mango/staging-adjust-adjust_trackers/{start_date}.jsonl",
        "dest": "channel_mapping",
    },
}

MANGO_USER_CHANNELS = {
    "type": "view",
    "params": {
        **BQ_PROJECT,
        "src": "mango_events",
        "src2": "channel_mapping",
        "dest": "user_channels",
    },
    "query": "user_channels",
}

SELECT_TABLE = {
    "type": "table",
    "params": {
        "src": "bigquery-public-data.usa_names.usa_1910_2013",
        "dest": "new",
        **BQ_PROJECT,
    },
    "query": "select_table",
    "append": True,
}
