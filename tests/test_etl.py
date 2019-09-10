import datetime
import sys
from argparse import Namespace

import etl
import numpy as np
import pytest
from pandas import DataFrame
from tasks import revenue
from tasks.base import EtlTask

SOURCES = {
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
    "gcs": {
        "bucket": "moz-fx-data",
        "prefix": "taipei/",
    },
    "fs": {
        "prefix": "test-data/",
        "file_format": "jsonl",
        "date_field": "utc_datetime",
    }
}


def cachedDataFrame(fpath, config) -> DataFrame:
    with open(fpath, "r") as f:
        raw = f.read()
        return EtlTask.convert_df(raw, config)


@pytest.mark.intgtest
def test_etl():
    sys.argv = ["./etl.py", "--debug"]
    etl.main()


@pytest.mark.unittest
def test_rps__global_package__fs():
    sys.argv = [
        "./etl.py",
        "--debug",
        "--config=test",
        "--task=rps",
        "--step=e",
        "--source=global_package",
        "--dest=fs",
    ]
    etl.main()


@pytest.mark.unittest
def test_revenue_google_search_extract_via_bq(mock_pdbq):
    queryResult = cachedDataFrame(
        "test-data/raw-revenue-google_search/2019-09-08.1.jsonl",
        {"file_format": "jsonl"})
    mock_pdbq.setQueryResult(queryResult)
    args = Namespace(config='test',
                     date=datetime.datetime(2019, 9, 8, 0, 0),
                     debug=True,
                     dest='fs',
                     loglevel=None,
                     period=30,
                     rm=False,
                     source='google_search',
                     step='e',
                     task='revenue')
    task = revenue.RevenueEtlTask(args, SOURCES, SCHEMA, DESTINATIONS)
    df = task.extract_via_bq("google_search", SOURCES["google_search"])
    assert isinstance(df, DataFrame)
    assert df.equals(queryResult)
