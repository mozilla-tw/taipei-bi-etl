"""Override debugging config here."""
from copy import deepcopy
from configs import rps

SOURCES = deepcopy(rps.SOURCES)

SCHEMA = deepcopy(rps.SCHEMA)

DESTINATIONS = deepcopy(rps.DESTINATIONS)

DESTINATIONS["gcs"]["bucket"] = "moz-taipei-bi"
DESTINATIONS["gcs"]["prefix"] = "mango/"

DESTINATIONS["fs"]["prefix"] = "./staging-data/"
