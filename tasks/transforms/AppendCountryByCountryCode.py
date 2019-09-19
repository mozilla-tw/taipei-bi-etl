"""Append column country by country code."""
from tasks.transform import ImmutableDataframe, transforms


class AppendCountryByCountryCode(transforms):
    """Transform to append Country column."""

    def __init__(self, *args, **kwargs):
        """Init."""
        super(AppendCountryByCountryCode, self).__init__(*args, **kwargs)

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
        df["country"] = df["country_code"]

        return ImmutableDataframe(df)
