from copy import deepcopy
from configs import adjust

SOURCES = deepcopy(adjust.SOURCES)

SCHEMA = deepcopy(adjust.SCHEMA)

DESTINATIONS = deepcopy(adjust.DESTINATIONS)
DESTINATIONS['gcs']['bucket'] = 'moz-taipei-bi'
DESTINATIONS['gcs']['prefix'] = 'mango/'

DESTINATIONS['fs']['prefix'] = './staging-data/'
