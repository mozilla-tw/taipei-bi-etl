"""Shared pytest fixtures."""
import datetime

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def revenue_sample_data():
    """Shared revenue test data."""
    ret = type("test", (), {})  # empty object
    date_str = "2019-09-06"
    ret.date = datetime.datetime(2019, 9, 6, 0, 0)
    tup = [
        ("google_search", "google_search"),
        ("NL", "IN"),
        ("Android", "Android"),
        (np.datetime64(date_str), np.datetime64(date_str)),
    ]
    ret.transformed_google_search = pd.DataFrame(
        dict(source=tup[0], country=tup[1], os=tup[2], utc_datetime=tup[3])
    )
    return ret
