# flake8: noqa
import numpy as np
import os

# FIXME: change output file name as 'latest' instead of 'date'
SOURCES = {
    "adjust_trackers": {
        "type": "api",
        "url": "https://api.adjust.com/kpis/v1/fngm2fg6tssg.json?attribution_source=dynamic&attribution_type=click&grouping=networks,campaigns,adgroups,creatives&human_readable_kpis=true&kpis=maus&reattributed=all&start_date=2019-01-01&end_date={end_date}&utc_offset=+00:00&user_token={api_key}",
        "request_interval": 1,
        "api_key": os.environ.get('ADJUST_API_KEY'),
        "json_path_nested": ["result_set.networks", "campaigns", "adgroups", "creatives"],
        "fields": ["name", "token"],
        "cache_file": True,
        "date_format": "%Y-%m-%d",
        "file_format": "json",
        "load": True,
        "write_latest": True,
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
    "gcs": {
        "bucket": "moz-taipei-bi",
        "prefix": "mango/",
    },
    "fs": {
        "prefix": "./data/",
        "file_format": "jsonl",
        "date_field": "execution_date",
    }
}
