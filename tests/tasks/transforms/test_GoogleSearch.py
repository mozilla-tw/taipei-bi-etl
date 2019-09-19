import logging

import numpy as np
import pytest
from pandas import DataFrame

from tasks.immutable_dataframe import ImmutableDataframe
from tasks.transforms.GoogleSearch import GoogleSearch

log = logging.getLogger(__name__)

tup = [
    ("google_search", "google_search"),
    ("NL", "IN"),
    ("Android", "Android"),
    (np.datetime64("2019-09-06"), np.datetime64("2019-09-06")),
    (105.0, 2.0),
    (6254.666632, 5323.402384),
]

schema = [
    ("source", np.dtype(object).type),
    ("country", np.dtype(object).type),
    ("os", np.dtype(object).type),
    ("utc_datetime", np.datetime64),
    ("tz", np.dtype(object).type),
    ("currency", np.dtype(object).type),
    ("sales_amount", np.dtype(float).type),
    ("payout", np.dtype(float).type),
    ("fx_defined1", np.dtype(object).type),
    ("fx_defined2", np.dtype(object).type),
    ("fx_defined3", np.dtype(object).type),
    ("fx_defined4", np.dtype(object).type),
    ("fx_defined5", np.dtype(object).type),
    ("conversion_status", np.dtype(object).type),
]


@pytest.mark.unittest
def test_GoogleSearch():
    df = DataFrame(
        dict(
            source=tup[0],
            country=tup[1],
            country_code=tup[1],
            os=tup[2],
            day=tup[3],
            event_count=tup[4],
            rps=tup[5],
        )
    )
    idf = ImmutableDataframe(df)
    data = idf | GoogleSearch(schema=schema)
    assert isinstance(data, ImmutableDataframe)
    result_df = data.to_dataframe()
    assert result_df["payout"].equals(df["event_count"] * df["rps"])
