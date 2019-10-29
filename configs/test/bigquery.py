from copy import deepcopy

from configs import bigquery
from utils.config import merge_config

BQ_PROJECT = {"dataset": "test", "location": "US", "project": "rocket-dev01"}


def set_debug_config():
    c = {
        key: value
        for key, value in bigquery.__dict__.items()
        if (not (key.startswith("__") or key.startswith("_") or key == "BQ_PROJECT"))
        and isinstance(value, dict)
    }
    for k, v in c.items():
        v_dbg = deepcopy(v)
        merge_config(v_dbg["params"], BQ_PROJECT)
        globals()[k] = v_dbg


set_debug_config()

# TODO: refactor the configs to more reconfigurable
globals()["MANGO_EVENTS"]["params"]["src"] = "rocket-dev01.mango_dev.mango_events"
# TODO: establish a bucket for testing assets
globals()["MANGO_CHANNEL_MAPPING"]["params"][
    "src"
] = "moz-taipei-bi-datasets/mango/staging-adjust-adjust_trackers/2019-10-03.jsonl"

SELECT_TABLE = {
    "type": "table",
    "params": {
        "src": "bigquery-public-data.usa_names.usa_1910_2013",
        "dest": "new",
        **BQ_PROJECT,
    },
    "query": "select_table",
    "append": True,
}
