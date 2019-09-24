"""Filter events and label with channels."""
from typing import Union

from pandas import DataFrame, Series

MAPPING: DataFrame = DataFrame()
COLUMNS = ["creative_token", "adgroup_token", "campaign_token", "network_token"]


class Network:

    @staticmethod
    def all_network(e: Series) -> Union[str, bool]:
        if e["settings_key"] == "pref_key_s_tracker_token":
            for col in COLUMNS:
                match = MAPPING[MAPPING[col] == e.get("settings_value")]
                match = match.reset_index()
                if len(match.index) > 0:
                    return match.at[0, "network_name"]
        return False
