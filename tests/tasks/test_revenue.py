import logging
from typing import Any, Dict
import pandas_gbq
import pytest
import requests
import datetime
import numpy as np
import utils.common
from google.cloud import storage
from google.cloud.storage import Bucket
import utils.config
from pandas import DataFrame
from tasks import base, revenue
from tests.utils import inject_fixtures
from argparse import Namespace

log = logging.getLogger(__name__)

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

task = "revenue"
inject_fixtures(
    globals(),
    task,
    {
        "prd": utils.config.get_configs(task, ""),
        "dbg": utils.config.get_configs(task, "")
    },
)


@pytest.mark.envtest
def test_read_api(req: requests, api_src: Dict[str, Any]):
    """Test calling APIs in source configs."""
    r = req.get(api_src["url"], allow_redirects=True)
    assert len(r.text) > 0


@pytest.mark.envtest
def test_read_gcs(gcs: storage.Client, gcs_src: Dict[str, Any]):
    """Test reading a GCS blob in source configs."""
    blobs = gcs.list_blobs(gcs_src["bucket"], prefix=gcs_src["prefix"])
    assert sum(1 for _ in blobs) > 0


@pytest.mark.envtest
def test_read_bq(pdbq: pandas_gbq, bq_src: Dict[str, Any]):
    """Test querying BigQuery in source configs."""
    df = pdbq.read_gbq(bq_src["sql"])
    assert len(df.index) > 0


@pytest.mark.envtest
def test_write_gcs(gcs_bucket: Bucket, gcs_dest: Dict[str, Any]):
    """Test writing a GCS blob in destination config."""
    blob = gcs_bucket.blob(gcs_dest["prefix"] + "test.txt")
    blob.upload_from_string("This is a test.")
    blob.delete()


@pytest.mark.unittest
def test_revenue_google_search_extract_via_bq(mock_pdbq):
    queryResult = utils.common.cachedDataFrame(
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


@pytest.mark.unittest
def test_revenue_google_search_extract(mock_pdbq):
    queryResult = utils.common.cachedDataFrame(
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
    task.extract()
    df = task.extracted[args.source]  # pylint: disable=no-member
    assert isinstance(df, DataFrame)
    assert df.equals(queryResult)
