"""Configurations for data source, data schema, and data destinations."""
# flake8: noqa
import numpy as np

SOURCES = {
    "google_search_rps": {
        "type": "gcs",
        "bucket": "moz-fx-data",
        "prefix": "taipei/",
        "path": "staging-rps-google_search_rps/",
        "filename": "2018-01-01.csv",
        "file_format": "csv",
    },
    "bukalapak": {
        "type": "api",
        "url": "https://api.test.com/resource.csv?api_key={api_key}&date_range={start_date}%2C{end_date}",
        "api_key": "j280wjf203jhf2083",
        "load": True,
        "request_interval": 1,
        "cache_file": True,
        "force_load_cache": False,
        "date_format": "%Y-%m-%d",
        "file_format": "json",
        "json_path": "response.data.data",
        "json_path_page_count": "response.data.pageCount",
        "page_size": 100,
        "country_code": "ID",  # for detecting timezone,
        "datetime_fields": ["Stat.date", "Stat.datetime", "Stat.session_datetime"],
    },
    "google_search": {
        "type": "bq",
        "project": "moz-fx-prod",
        "dataset": "telemetry",
        "table": "focus_event",
        "udf_js": ["json_extract_events"],
        "query": "revenue_search_events",
        "load": True,
        "date_format": "%Y-%m-%d",
    },
}
SCHEMA = [
    ("source", np.dtype(object).type),
    ("country", np.dtype(object).type),
    ("os", np.dtype(object).type),
    ("utc_datetime", np.datetime64),
    ("tz", np.dtype(object).type),
    ("currency", np.dtype(object).type),
    ("sales_amount", np.dtype(float).type),
    ("payout", np.dtype(float).type),
    ("fx_defined1", np.dtype(object).type),
    ("fx_defined2", np.dtype(object).type),
    ("fx_defined3", np.dtype(object).type),
    ("fx_defined4", np.dtype(object).type),
    ("fx_defined5", np.dtype(object).type),
    ("conversion_status", np.dtype(object).type),
]
DESTINATIONS = {
    "gcs": {"bucket": "moz-fx-data", "prefix": "taipei/"},
    "fs": {
        "prefix": "./data/",
        "file_format": "jsonl",
        "date_field": "utc_datetime",
    },
}
