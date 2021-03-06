"""Override debugging config here."""
from copy import deepcopy
from configs import revenue

SOURCES = deepcopy(revenue.SOURCES)

SCHEMA = deepcopy(revenue.SCHEMA)

DESTINATIONS = deepcopy(revenue.DESTINATIONS)

DESTINATIONS["gcs"]["bucket"] = "moz-taipei-bi-datasets"
DESTINATIONS["gcs"]["prefix"] = "mango/"

DESTINATIONS["fs"]["prefix"] = "./debug-data/"
