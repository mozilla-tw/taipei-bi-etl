"""Filter events and label with features."""
from pandas import Series


class Feature:

    @staticmethod
    def search(e: Series):
        if (
            e["method"] in ["type_query", "select_query"]
            and e["object"] == "search_bar"
            and e["value"] in (None, "")
        ):
            return "search"
        return False
