"""Configurations for data source, data schema, and data destinations."""
# flake8: noqa
import numpy as np
import os

SOURCES = {
    "global_package": {
        "type": "const",
        "values": [{"package": 539168000}],
    },
    "cb_index": {
        "type": "api",
        "url": "https://www.chartboost.com/wp-content/uploads/2019/08/CPI_July_2019.csv",
        "api_key": "",
        "header": ["platform", "category", "country_code", "country", "cpi", "month"],
        "cache_file": True,
        "date_format": "%Y-%m-%d",
        "file_format": "csv",
    },
    "fb_index": {
        "type": "api",
        "url": "https://crossborderinsightsfinder.com/wp-json/fb/v1/countries?vertical_id={iterator}&ad_objective_id=3&date_range={start_date}%2C{end_date}",
        "api_key": "",
        "iterator": range(1, 18),
        "cache_file": True,
        "date_format": "%Y-%m-%d",
        "file_format": "json",
    },
    "google_search_rps": {
        "type": "api",
        "url": "https://sql.telemetry.mozilla.org/api/queries/64294/results.csv?api_key={api_key}",
        "api_key": os.environ.get('STMO_API_KEY'),
        "cache_file": True,
        "load": True,
        "force_load_cache": False,
        "date_format": "%Y-%m-%d",
        "file_format": "csv",
    },
}
SCHEMA = [
    ("country", np.dtype(object).type),
    ("volume",  np.dtype(int).type),
    ("cost_idx_base",  np.dtype(float).type),
    ("cost_idx_latest", np.dtype(float).type),
    ("cost_idx_cb", np.dtype(float).type),
    ("rps", np.dtype(float).type),
    ("rps_cb", np.dtype(float).type),
    ("cb_rps_ratio", np.dtype(float).type),
]
DESTINATIONS = {
    "gcs": {
        "bucket": "moz-fx-data-derived-datasets-analysis",
        "prefix": "taipei-bi/",
    },
    "fs": {
        "prefix": "./data/",
        "file_format": "csv",
    }
}
