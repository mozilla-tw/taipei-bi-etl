"""Filter events and label with channels."""
from pandas import DataFrame, Series


class Network:

    @staticmethod
    def all_networks(e: Series):
        # TODO: map channel name from API
        if e["tracker_token"] == "test match":
            return "Organic"
        return False
