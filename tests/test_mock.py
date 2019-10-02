import logging

import pandas as pd
import pandas_gbq
import pytest
import requests
from google.cloud import bigquery, storage

import utils.file

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
def test_mock_readwrite(mock_readwrite):
    """Testing mock_readwrite fixture."""
    filename1 = "test1.txt"
    filename2 = "test2.txt"
    STR1 = "test1"
    STR2 = "test2"
    utils.file.write_string(filename1, STR1)
    utils.file.write_string(filename2, STR2)
    data1 = utils.file.read_string(filename1)
    data2 = utils.file.read_string(filename2)
    assert data1 == STR1
    assert data2 == STR2


@pytest.mark.mocktest
def test_mock_requests(mock_requests):
    """Testing mock_requests fixture."""
    CONTENT = "test"
    URL = "http://test/"
    mock_requests.setContent(URL, CONTENT)
    r = requests.get(URL)
    assert CONTENT == r.text


@pytest.mark.mocktest
def test_mock_pdbq(mock_pdbq):
    """Testing mock_pdbq fixture."""
    QUERY = "select * from test"
    tup = [("google_search", "google_search"), ("NL", "IN"), ("Android", "Android")]
    EXPECTED = pd.DataFrame(dict(source=tup[0], country=tup[1], os=tup[2]))
    mock_pdbq.setQueryResult(QUERY, EXPECTED)
    df = pandas_gbq.read_gbq(QUERY)
    log.debug(df)
    assert isinstance(df, pd.DataFrame)
    assert df.equals(EXPECTED)


@pytest.mark.mocktest
def test_mock_bigquery(mock_bigquery):
    """Testing mock_bigquery fixture."""
    client = bigquery.Client()
    assert client


@pytest.mark.mocktest
def test_mock_gcs(mock_gcs, mock_readwrite):
    """Testing mock_gcs fixture."""
    bucket_name = "bucket"
    file_list = ["test1.txt", "test2.txt", "test3.txt"]
    tempfile = "tempfile"
    for f in file_list:
        utils.file.write_string(f, f)
    gcs = storage.Client()
    bucket = gcs.create_bucket(bucket_name)
    for f in file_list:
        blob = bucket.blob(f)
        log.debug(blob.name)
        blob.upload_from_filename(f)

    # download first file and validate the content
    blob = bucket.blob(file_list[0])
    blob.download_to_filename(tempfile)
    assert utils.file.read_string(tempfile) == file_list[0]

    # validate the objects in bucket
    blobs = gcs.list_blobs(bucket_name)
    assert sorted([b.name for b in blobs]) == sorted(file_list)
