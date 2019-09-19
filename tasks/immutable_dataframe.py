"""Immutable Dataframe."""
import logging

from pandas import DataFrame

log = logging.getLogger(__name__)

# TODO: Obselete this data type, use Apache Beam PCollection instead


class ImmutableDataframe:
    """Immutable Dataframe."""

    def __init__(self, idf: DataFrame):
        """Init."""
        self._df = idf.copy()

    def to_dataframe(self) -> DataFrame:
        """Return a pandas DataFrame."""
        return self._df.copy()

    def __or__(self, ptransform):
        """Overload or operator as transform operator."""
        return ptransform.process(self)

    def equals(self, other):
        """Check equal."""
        return other.to_dataframe().equals(self._df)

    def __str__(self):
        """Overload print representation."""
        return self._df.__str__()
