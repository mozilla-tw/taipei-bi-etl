"""Shared pytest fixtures."""
import logging
import pytest

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
