from argparse import Namespace
from typing import Dict
import utils.config


DEFAULTS = {}


class BqTask:
    def __init__(self):
        pass

    def run(self):
        pass


class BqGcsTask(BqTask):
    def __init__(self, config: Dict):
        super().__init__()

    def run(self):
        super().run()


class BqTableTask(BqTask):
    def __init__(self, config: Dict):
        super().__init__()

    def run(self):
        super().run()


class BqViewTask(BqTask):
    def __init__(self, config: Dict):
        super().__init__()

    def run(self):
        super().run()


def get_task_by_config(config: Dict):
    assert "type" in config, "Task type is required in BigQuery config."
    if config["type"] == "gcs":
        return BqGcsTask(config)
    elif config["type"] == "view":
        return BqViewTask(config)
    elif config["type"] == "table":
        return BqTableTask(config)


def main(args: Namespace):
    """Take args and pass them to BqTask.

    :param args: args passed from command line, see `base.get_arg_parser()`
    """
    config_name = ""
    if args.debug:
        config_name = "debug"
    if args.config:
        config_name = args.config
    configs = utils.config.get_configs("bigquery", config_name)
    get_task_by_config(configs.MANGO_EVENTS).run()
    get_task_by_config(configs.MANGO_EVENTS_UNNESTED).run()


if __name__ == "__main__":
    arg_parser = utils.config.get_arg_parser(**DEFAULTS)
    main(arg_parser.parse_args())
