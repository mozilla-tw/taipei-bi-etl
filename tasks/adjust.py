"""Adjust ETL task."""
import datetime
from argparse import Namespace
from typing import Dict, Any, List, Tuple
from tasks import base
import numpy as np
from utils.config import get_configs, get_arg_parser
import logging

from utils.marshalling import lookback_dates

log = logging.getLogger(__name__)

DEFAULTS = {"date": lookback_dates(datetime.datetime.utcnow(), 1)}


class AdjustEtlTask(base.EtlTask):
    """ETL task to compute Adjust from events."""

    def __init__(
        self,
        args: Namespace,
        sources: Dict[str, Any],
        schema: List[Tuple[str, np.generic]],
        destinations: Dict[str, Any],
    ):
        """Initialize Adjust ETL task.

        :param args: args passed from command line,
        see `get_arg_parser()`
        :param sources: data source to be extracted,
        specified in task config, see `configs/*.py`
        :param schema: the target schema to load to.
        :param destinations: destinations to load data to,
        specified in task config, see `configs/*.py`
        """
        super().__init__(args, sources, schema, destinations, "staging", "adjust")

    def transform_adjust_trackers(self, adjust_trackers):
        """Transform Adjust data."""
        # trasnform here
        adjust_trackers["execution_date"] = self.current_date
        adjust_trackers["execution_date"] = adjust_trackers["execution_date"].astype(
            "datetime64[ns]"
        )
        return adjust_trackers


def main(args: Namespace):
    """Take args and pass them to AdjustEtlTask.

    :param args: args passed from command line, see `base.get_arg_parser()`
    """
    config_name = ""
    if args.debug:
        config_name = "debug"
    if args.config:
        config_name = args.config
    configs = get_configs("adjust", config_name)
    task = AdjustEtlTask(args, configs.SOURCES, configs.SCHEMA, configs.DESTINATIONS)
    log.info("Running Adjust Task.")
    task.run()
    log.info("Adjust Task Finished.")


if __name__ == "__main__":
    arg_parser = get_arg_parser(**DEFAULTS)
    main(arg_parser.parse_args())
