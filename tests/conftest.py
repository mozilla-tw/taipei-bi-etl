"""Shared pytest fixtures."""
import builtins
import logging

import pandas_gbq
import pytest
import requests
from .mockio import MockIO
from .mockbigquery import MockBigqueryClient
from google.cloud import storage
from google.cloud import bigquery
from pandas import DataFrame

log = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def req():
    """Pytest fixture for accessing requests library."""
    import requests

    return requests


@pytest.fixture(scope="module")
def gcs():
    """Pytest fixture for accessing GCS library."""
    from google.cloud import storage

    return storage.Client()


@pytest.fixture(scope="module")
def pdbq():
    """Pytest fixture for accessing PandasGBQ library."""
    import pandas_gbq

    return pandas_gbq


# TODO: from docs.pytest.org
# Be advised that it is not recommended to patch builtin functions such as
# open, compile, etc., because it might break pytest’s internals.
# If that’s unavoidable, passing --tb=native, --assert=plain and --capture=no
# might help although there’s no guarantee.
#
# defining mock objects
@pytest.fixture
def mock_io(monkeypatch):
    """Mock file IO object."""
    mock_io = MockIO()
    monkeypatch.setattr(builtins, "open", mock_io.open)

    return mock_io


@pytest.fixture
def mock_requests(monkeypatch):
    """Mock http request object."""
    # defining mock objects
    class MockResponse:
        def get_text(self):
            log.warning("mock_response.text")
            return "test response text"

        text = property(get_text)

    mock_response = MockResponse()

    def mock_get(url, **kwargs):
        log.warning("mock_get(%s)" % url)
        return mock_response

    monkeypatch.setattr(requests, "get", mock_get)

    return mock_response


@pytest.fixture
def mock_pdbq(monkeypatch):
    """Mock Pandas GBQ object."""
    # defining mock objects
    class MockResponse:
        df = DataFrame()

        def setQueryResult(self, input: DataFrame):
            self.df = input.copy()

    mock = MockResponse()

    def mock_read_gbq(query, **kwargs):
        log.warning("mock_read_gbq(%s)" % query)
        return mock.df

    monkeypatch.setattr(pandas_gbq, "read_gbq", mock_read_gbq)

    return mock


@pytest.fixture
def mock_bigquery(monkeypatch):
    """Mock google-cloud-bigquery object."""
    monkeypatch.setattr(bigquery, "Client", MockBigqueryClient)


@pytest.fixture
def mock_gcs(monkeypatch):
    """Mock GCS client object."""
    # defining mock objects
    class MockBlob:
        def get_name(self):
            log.warning("mock_blob.name")
            return "test blob name"

        def upload_from_filename(self, filename, **kwargs):
            log.warning("mock_blob.upload_from_filename(%s)" % filename)

        def download_to_filename(self, filename, **kwargs):
            log.warning("mock_blob.download_to_filename(%s)" % filename)

        name = property(get_name)

    class MockBucket:
        def blob(self, blob_name, **kwargs):
            log.warning("mock_bucket.bucket(%s)" % blob_name)
            return MockBlob()

    class MockGCS:
        def bucket(self, bucket_name, **kwargs):
            log.warning("mock_gcs.bucket(%s)" % bucket_name)
            return MockBucket()

        def list_blobs(self, bucket_name, **kwargs):
            log.warning("mock_gcs.list_blobs(%s)" % bucket_name)
            return [MockBlob(), MockBlob(), MockBlob()]

    mock_gcs_client = MockGCS()

    def mock_client(**kwargs):
        log.warning("mock_gcs_client")
        return mock_gcs_client

    monkeypatch.setattr(storage, "Client", mock_client)

    return mock_gcs_client
