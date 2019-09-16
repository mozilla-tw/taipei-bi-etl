import pytest
import logging
from pandas import DataFrame

log = logging.getLogger(__name__)


class MockBigqueryClient():
    def query(self, query, **kwargs):
        return MockBigqueryJobQueryJob()


class MockBigqueryJobQueryJob():
    def to_dataframe(self, query, **kwargs):
        return DataFrame()
