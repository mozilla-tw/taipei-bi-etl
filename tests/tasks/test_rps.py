import time
import logging
from typing import Dict, Any
import requests
from google.cloud.storage import Bucket
from configs import rps as cfg
from configs.debug import rps as cfg_dbg
from tests.utils import inject_fixtures

log = logging.getLogger(__name__)

inject_fixtures(globals(), "rps", {"prd": cfg, "dbg": cfg_dbg})


def test_read_api(req: requests, api_src: Dict[str, Any]):
    """Test calling APIs in source configs."""
    r = req.get(api_src["url"], allow_redirects=True)
    assert len(r.text) > 0


def test_write_gcs(gcs_bucket: Bucket, gcs_dest: Dict[str, Any]):
    """Test writing a GCS blob in destination config."""
    blob = gcs_bucket.blob(gcs_dest["prefix"] + "test.txt")
    blob.upload_from_string("This is a test.")
    blob.delete()
