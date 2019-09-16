import logging
from typing import Any, Dict
import pandas_gbq
import pytest
import requests
import datetime
import utils.common
from google.cloud import storage
from google.cloud.storage import Bucket
from pandas import DataFrame
from tasks import revenue
from tests.utils import inject_fixtures
from argparse import Namespace
from utils.config import get_configs

log = logging.getLogger(__name__)

cfg = get_configs("revenue", "test")

task = "revenue"
inject_fixtures(
    globals(),
    task,
    {
        "prd": utils.config.get_configs(task, ""),
        "dbg": utils.config.get_configs(task, ""),
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
        {"file_format": "jsonl"},
    )
    mock_pdbq.setQueryResult(queryResult)
    args = Namespace(
        config="test",
        date=datetime.datetime(2019, 9, 8, 0, 0),
        debug=True,
        dest="fs",
        loglevel=None,
        period=30,
        rm=False,
        source="google_search",
        step="e",
        task="revenue",
    )
    task = revenue.RevenueEtlTask(args, cfg.SOURCES, cfg.SCHEMA, cfg.DESTINATIONS)
    df = task.extract_via_bq("google_search", cfg.SOURCES["google_search"])
    assert isinstance(df, DataFrame)
    assert df.equals(queryResult)


@pytest.mark.unittest
def test_revenue_google_search_extract(mock_pdbq):
    queryResult = utils.common.cachedDataFrame(
        "test-data/raw-revenue-google_search/2019-09-08.1.jsonl",
        {"file_format": "jsonl"},
    )
    mock_pdbq.setQueryResult(queryResult)
    args = Namespace(
        config="test",
        date=datetime.datetime(2019, 9, 8, 0, 0),
        debug=True,
        dest="fs",
        loglevel=None,
        period=30,
        rm=False,
        source="google_search",
        step="e",
        task="revenue",
    )
    task = revenue.RevenueEtlTask(args, cfg.SOURCES, cfg.SCHEMA, cfg.DESTINATIONS)
    task.extract()
    df = task.extracted[args.source]  # pylint: disable=no-member
    assert isinstance(df, DataFrame)
    assert df.equals(queryResult)
