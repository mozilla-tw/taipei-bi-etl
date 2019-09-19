import logging

import pandas as pd
import pandas_gbq
import pytest
import requests
from google.cloud import bigquery, storage

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
    CONTENT = "test"
    URL = "http://test/"
    mock_requests.setContent(URL, CONTENT)
    r = requests.get(URL)
    log.warning(r.text)
    assert CONTENT == r.text


@pytest.mark.mocktest
def test_mock_pdbq(mock_pdbq):
    """Testing mock_pdbq fixture."""
    QUERY = "select * from test"
    tup = [("google_search", "google_search"), ("NL", "IN"), ("Android", "Android")]
    EXPECTED = pd.DataFrame(dict(source=tup[0], country=tup[1], os=tup[2]))
    mock_pdbq.setQueryResult(QUERY, EXPECTED)
    df = pandas_gbq.read_gbq(QUERY)
    log.warning(df)
    assert isinstance(df, pd.DataFrame)
    assert df.equals(EXPECTED)


@pytest.mark.mocktest
def test_mock_bigquery(mock_bigquery):
    """Testing mock_bigquery fixture."""
    client = bigquery.Client()
    assert client


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
