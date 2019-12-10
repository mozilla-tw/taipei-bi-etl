from copy import deepcopy
from configs import bigquery
from utils.config import merge_config

BQ_PROJECT = {
    "project": "rocket-dev01",
    # "project": "taipei-bi",
    "location": "US",
    "dataset": "mango_dev3",
    # "dataset": "mango_dev",
    # "dataset": "mango_staging",
}


def set_debug_config():
    c = {
        key: value
        for key, value in bigquery.__dict__.items()
        if (
           not (key.startswith("__") or key.startswith("_") or key == "BQ_PROJECT")
        ) and isinstance(value, dict)
    }
    for k, v in c.items():
        v_dbg = deepcopy(v)
        merge_config(v_dbg["params"], BQ_PROJECT)
        globals()[k] = v_dbg


set_debug_config()

globals()["MANGO_CHANNEL_MAPPING"]["params"]["src"] = "moz-taipei-bi-datasets/mango/staging-adjust-adjust_trackers/{start_date}.jsonl"
globals()["GOOGLE_RPS"]["params"]["src"] = "moz-taipei-bi-datasets/mango/staging-rps-google_search_rps/2018-01-01.csv"
globals()["MANGO_REVENUE_BUKALAPAK"]["params"]["src"] = "moz-taipei-bi-datasets/mango/staging-revenue-bukalapak/{start_date}.jsonl"
globals()["MANGO_USER_CHANNELS"]["create_view_alt"] = True
globals()["MANGO_FEATURE_ROI"]["create_view_alt"] = True
globals()["MANGO_USER_RFE"]["create_view_alt"] = True
globals()["MANGO_USER_RFE_SESSION"]["create_view_alt"] = True
globals()["MANGO_ACTIVE_USER_COUNT"]["create_view_alt"] = True
globals()["MANGO_COHORT_RETAINED_USERS"]["create_view_alt"] = True
globals()["MANGO_FEATURE_COHORT_DATE"]["create_view_alt"] = True
