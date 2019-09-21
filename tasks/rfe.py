from tasks import base
#from configs import feature_mapping
from configs.mapping import feature
from configs import rfe
from configs.debug import rfe as rfe_dbg

import pandas as pd
import pandasql as ps
import numpy as np
from pandas.io.json import json_normalize


DEFAULTS = {}


class RfeEtlTask(base.EtlTask):

    def __init__(self, args, sources, schema, destinations):
        super().__init__(args, sources, schema, destinations, 'staging', 'rfe')

    def transform_rfe(self, source, config):
        # trasnform here
        df = self.extracted[source]
        print(df)
        return df

    def transform_test(self, source, config):
        # trasnform here
        df = self.extracted[source]
        print(df)
        return df


def main(args):
    srcs = rfe.SOURCES if not args.debug else rfe_dbg.SOURCES
    dests = rfe.DESTINATIONS if not args.debug else rfe_dbg.DESTINATIONS
    task = RfeEtlTask(args, srcs, rfe.SCHEMA, dests)
    task.run()


if __name__ == "__main__":
    arg_parser = base.get_arg_parser(**DEFAULTS)
    main(arg_parser.parse_args())
