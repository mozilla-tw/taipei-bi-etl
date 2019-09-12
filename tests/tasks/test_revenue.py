import logging
from typing import Any, Dict
import pandas_gbq
import pytest
import requests
from google.cloud import storage
from google.cloud.storage import Bucket
import utils.config
from tests.utils import inject_fixtures

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
