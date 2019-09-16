"""Mock Bigquery."""
import logging
from pandas import DataFrame

log = logging.getLogger(__name__)


class MockBigqueryClient:
    """Mock Object Class for bigquery client."""

    def query(self, query, **kwargs):
        """Query."""
        return MockBigqueryJobQueryJob()


class MockBigqueryJobQueryJob:
    """Mock Object Class for bigquery query job."""

    def to_dataframe(self, query, **kwargs):
        """Convert to pandas dataframe."""
        return DataFrame()
