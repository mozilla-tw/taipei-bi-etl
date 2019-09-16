import datetime
import logging
from argparse import Namespace
from typing import Any, Dict

import pandas_gbq
import pytest
import requests
from google.cloud import storage
from google.cloud.storage import Bucket
from pandas import DataFrame

import utils.common
from tasks import revenue
from tests.utils import inject_fixtures
from utils.config import DEFAULT_DATETIME_FORMAT, get_configs

log = logging.getLogger(__name__)

task = "revenue"
inject_fixtures(
    globals(),
    task,
    {
        "prd": utils.config.get_configs(task, ""),
        "dbg": utils.config.get_configs(task, ""),
    },
)
cfg = get_configs(task, "test")


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


@pytest.mark.unittest
def test_revenue_load_to_fs(mock_io, revenue_sample_data):
    source = "google_search"
    stage = "staging"
    args = Namespace(
        config="test",
        date=datetime.datetime(2019, 9, 8, 0, 0),
        debug=True,
        dest="fs",
        loglevel=None,
        period=30,
        rm=False,
        source="google_search",
        step="l",
        task="revenue",
    )
    task = revenue.RevenueEtlTask(args, cfg.SOURCES, cfg.SCHEMA, cfg.DESTINATIONS)
    task.transformed[
        source
    ] = revenue_sample_data.transformed_google_search  # pylint: disable=no-member
    task.load_to_fs(source, cfg.SOURCES[source], stage)
    expected_filename = task.get_or_create_filepath(
        source, cfg.SOURCES[source], stage, "fs", None, revenue_sample_data.date
    )
    with open(expected_filename, "r") as f:
        data = f.read()
    expected = revenue_sample_data.transformed_google_search
    expected["utc_datetime"] = expected["utc_datetime"].dt.strftime(
        DEFAULT_DATETIME_FORMAT
    )
    assert data == task.convert_format(expected)


@pytest.mark.unittest
def test_revenue_load(mock_io, revenue_sample_data):
    source = "google_search"
    stage = "staging"
    args = Namespace(
        config="test",
        date=datetime.datetime(2019, 9, 8, 0, 0),
        debug=True,
        dest="fs",
        loglevel=None,
        period=30,
        rm=False,
        source="google_search",
        step="l",
        task="revenue",
    )
    # load only google_search
    cfg.SOURCES = {"google_search": cfg.SOURCES["google_search"]}
    task = revenue.RevenueEtlTask(args, cfg.SOURCES, cfg.SCHEMA, cfg.DESTINATIONS)
    task.transformed[
        source
    ] = revenue_sample_data.transformed_google_search  # pylint: disable=no-member
    task.load()
    expected_filename = task.get_or_create_filepath(
        source, cfg.SOURCES[source], stage, "fs", None, revenue_sample_data.date
    )
    with open(expected_filename, "r") as f:
        data = f.read()
    expected = revenue_sample_data.transformed_google_search
    expected["utc_datetime"] = expected["utc_datetime"].dt.strftime(
        DEFAULT_DATETIME_FORMAT
    )
    assert data == task.convert_format(expected)
