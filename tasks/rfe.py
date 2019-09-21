"""RFE task."""
from argparse import Namespace
from typing import Dict, Any, List, Tuple
from tasks import base

# import pandas as pd
# import pandasql as ps
import numpy as np

from utils.config import get_configs, get_arg_parser

DEFAULTS = {}


class RfeEtlTask(base.EtlTask):
    """ETL task to compute RFE from events."""

    def __init__(
        self,
        args: Namespace,
        sources: Dict[str, Any],
        schema: List[Tuple[str, np.generic]],
        destinations: Dict[str, Any],
    ):
        """Initialize RPS ETL task.

        :param args: args passed from command line,
        see `get_arg_parser()`
        :param sources: data source to be extracted,
        specified in task config, see `configs/*.py`
        :param schema: the target schema to load to.
        :param destinations: destinations to load data to,
        specified in task config, see `configs/*.py`
        """
        super().__init__(args, sources, schema, destinations, "staging", "rfe")

    def transform_rfe(self, source, config):
        """Transform RFE data."""
        # trasnform here
        df = self.extracted[source]
        print(df)
        return df

    def transform_test(self, source, config):
        """Transform test data."""
        # trasnform here
        df = self.extracted[source]
        print(df)
        return df


def main(args):
    """Take args and pass them to RfeEtlTask.

    :param args: args passed from command line, see `base.get_arg_parser()`
    """
    config_name = ""
    if args.debug:
        config_name = "debug"
    if args.config:
        config_name = args.config
    configs = get_configs("rfe", config_name)
    task = RfeEtlTask(args, configs.SOURCES, configs.SCHEMA, configs.DESTINATIONS)
    task.run()


if __name__ == "__main__":
    arg_parser = get_arg_parser(**DEFAULTS)
    main(arg_parser.parse_args())
