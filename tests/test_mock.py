import pytest
import requests
import logging
import pandas_gbq
from google.cloud import storage
from google.cloud import bigquery

log = logging.getLogger(__name__)


@pytest.mark.mocktest
def test_mock_io(mock_io):
    """Testing mock_io fixture."""
    filename1 = "test1.txt"
    filename2 = "test2.txt"
    STR1 = "test1"
    STR2 = "test2"
    with open(filename1, "w") as f:
        f.write(STR1)
    with open(filename2, "w") as f:
        f.write(STR2)
    with open(filename1, "r") as f:
        data1 = f.read()
    with open(filename2, "r") as f:
        data2 = f.read()
    assert data1 == STR1
    assert data2 == STR2


@pytest.mark.mocktest
def test_mock_requests(mock_requests):
    """Testing mock_requests fixture."""
    r = requests.get("http://test/")
    log.warning(r.text)


@pytest.mark.mocktest
def test_mock_pdbq(mock_pdbq):
    """Testing mock_pdbq fixture."""
    df = pandas_gbq.read_gbq("select * from test")
    log.warning(df)


@pytest.mark.mocktest
@pytest.mark.mocktest
def test_mock_gcs(mock_gcs):
    """Testing mock_gcs fixture."""
    gcs = storage.Client()
    bucket = gcs.bucket("test bucket")
    blob = bucket.blob("test blob")
    log.warning(blob.name)
    blob.upload_from_filename("test file")
    blob.download_to_filename("test file")
    blobs = gcs.list_blobs("test bucket")
    for b in blobs:
        log.warning(b.name)
