# flake8: noqa
import numpy as np

SOURCES = {
    "adjust_trackers": {
        "type": "api",
        "url": "https://api.adjust.com/kpis/v1/abc.json&user_token={api_key}",
        "request_interval": 1,
        "api_key": "xyz",
        "json_path_nested": ["result_set.networks", "campaigns", "adgroups", "creatives"],
        "fields": ["name", "token"],
        "cache_file": True,
        "date_format": "%Y-%m-%d",
        "file_format": "json",
        "load": True
    },
}

SCHEMA = [
    ("network_name", np.dtype(object).type),
    ("network_token",  np.dtype(object).type),
    ("campaign_name", np.dtype(object).type),
    ("campaign_token",  np.dtype(object).type),
    ("adgroup_name", np.dtype(object).type),
    ("adgroup_token", np.dtype(object).type),
    ("creative_name", np.dtype(object).type),
    ("creative_token", np.dtype(object).type),
    ("execution_date", np.datetime64),
]

DESTINATIONS = {
    "gcs": {"bucket": "moz-fx-data", "prefix": "taipei/"},
    "fs": {
        "prefix": "./data/",
        "file_format": "jsonl",
        "date_field": "execution_date",
    }
}
