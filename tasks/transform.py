"""Transform base class."""
import logging

from tasks.immutable_dataframe import ImmutableDataframe

log = logging.getLogger(__name__)

# TODO: Obselete this transform class, use Apache Beam
# apache_beam.transforms.core instead


class transforms:
    """Transform base class."""

    def __init__(self, *args, **kwargs):
        """Init."""
        pass

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
        raise NotImplementedError
