import logging

import pytest
from pandas import DataFrame

from tasks.immutable_dataframe import ImmutableDataframe
from tasks.transforms.AppendCountryByCountryCode import AppendCountryByCountryCode

log = logging.getLogger(__name__)


@pytest.mark.unittest
def test_AppendCountryByCountryCode():
    tup = [("google_search", "google_search"), ("NL", "IN"), ("Android", "Android")]
    df = DataFrame(dict(source=tup[0], country_code=tup[1], os=tup[2]))
    idf = ImmutableDataframe(df)
    data = idf | AppendCountryByCountryCode()
    assert isinstance(data, ImmutableDataframe)
    assert data.equals(
        ImmutableDataframe(
            DataFrame(
                dict(source=tup[0], country_code=tup[1], os=tup[2], country=tup[1])
            )
        )
    )
