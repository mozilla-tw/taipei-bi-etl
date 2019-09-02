import time
import logging

log = logging.getLogger(__name__)


def test_read_api(req, api_src):
    """Test calling APIs in source configs."""
    time.sleep(1)
    r = req.get(api_src["url"], allow_redirects=True)
    assert len(r.text) > 0


def test_write_gcs(gcs_bucket, gcs_dest):
    """Test writing a GCS blob in destination config."""
    blob = gcs_bucket.blob(gcs_dest["prefix"] + "test.txt")
    blob.upload_from_string("This is a test.")
    blob.delete()
