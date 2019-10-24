from copy import deepcopy

from configs import bigquery

BQ_PROJECT = {"dataset": "test", "location": "US", "project": "rocket-dev01"}

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

MANGO_EVENTS = {**deepcopy(bigquery.MANGO_EVENTS), "id": BQ_PROJECT}

MANGO_EVENTS_UNNESTED = {**deepcopy(bigquery.MANGO_EVENTS_UNNESTED), "id": BQ_PROJECT}

MANGO_EVENTS_FEATURE_MAPPING = {
    **deepcopy(bigquery.MANGO_EVENTS_FEATURE_MAPPING),
    "id": BQ_PROJECT,
}

CHANNEL_MAPPING = {**deepcopy(bigquery.CHANNEL_MAPPING), "id": BQ_PROJECT}

USER_CHANNELS = {**deepcopy(bigquery.USER_CHANNELS), "id": BQ_PROJECT}

for event in [
    MANGO_EVENTS,
    MANGO_EVENTS_UNNESTED,
    MANGO_EVENTS_FEATURE_MAPPING,
    CHANNEL_MAPPING,
    USER_CHANNELS,
]:
    event["params"]["dataset"] = BQ_PROJECT["dataset"]
    event["params"]["project"] = BQ_PROJECT["project"]
