"""Append column country by country code."""
import numpy as np
from pandas import DataFrame

from tasks.transform import ImmutableDataframe, transforms
from utils.marshalling import get_country_tz_str


class GoogleSearch(transforms):
    """Transform to append Country column."""

    def __init__(self, *args, **kwargs):
        """Init."""
        super(GoogleSearch, self).__init__(*args, **kwargs)
        self.schema = kwargs["schema"]

    def process(
        self, element: ImmutableDataframe, *args, **kwargs
    ) -> ImmutableDataframe:
        """
        Process function.

        Args:
          element: The element to be processed
          *args: side inputs
          **kwargs: other keyword arguments.

        """
        df = element.to_dataframe()
        td = DataFrame(np.empty(0, dtype=np.dtype(self.schema)))
        td["os"] = df["os"]
        td["country"] = df["country_code"]
        # workaround for datetime64 validation since `datetime64[ns, UTC]`
        # will raise "TypeError: data type not understood"
        td["utc_datetime"] = df["day"].astype("datetime64[ns]")
        td["tz"] = df["country"].apply(lambda x: get_country_tz_str(x))
        td["payout"] = df["event_count"] * df["rps"]
        td["payout"] = td["payout"].fillna(0)
        td["sales_amount"] = td["sales_amount"].fillna(0)
        td["source"] = td["source"].fillna("google_search")
        td["currency"] = td["currency"].fillna("USD")
        return ImmutableDataframe(td)
