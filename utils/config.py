"""Config utilities."""
import datetime
import importlib
from argparse import ArgumentParser
from typing import Optional, Callable
import logging

log = logging.getLogger(__name__)

DEFAULT_DATE_FORMAT = "%Y-%m-%d"
DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_TZ_FORMAT = "%+03d:00"
EXT_REGEX = "((\\.[*0-9A-z]+)?\\.[A-z0-9]+$)"
DEFAULT_PATH_FORMAT = "{prefix}{stage}-{task}-{source}"
INJECTED_MAPPINGS = dict()


def get_configs(mod: str, pkg: str = "") -> Optional[Callable]:
    """Get configs by module name and package name.

    :rtype: Callable
    :param mod: the name of the ETL module
    :param pkg: the package of the ETL module
    :return: the config module
    """
    module = ""
    if pkg == "":
        module = "%s.%s" % ("configs", mod)
    else:
        module = "%s.%s.%s" % ("configs", pkg, mod)
    try:
        return importlib.import_module(module)
    except ModuleNotFoundError:
        log.warning("Config module %s not found." % module)
    return None


def get_arg_parser(**kwargs) -> ArgumentParser:
    """Parse arguments passed in EtlTask.

    --help will list all argument descriptions

    :rtype: ArgumentParser
    :return: properly configured argument parser to accept arguments
    """
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--debug",
        default=False if "debug" not in kwargs else kwargs["debug"],
        action="store_true",
        help="Run tasks in DEBUG mode, will use debugging configs in /configs/debug/*",
    )
    parser.add_argument(
        "--loglevel",
        default=None if "loglevel" not in kwargs else kwargs["loglevel"],
        help="Set log level by name.",
    )
    parser.add_argument(
        "--config",
        default="" if "config" not in kwargs else kwargs["config"],
        help="The ETL config to use.",
    )
    parser.add_argument(
        "--task",
        default=None if "task" not in kwargs else kwargs["task"],
        help="The ETL task to run.",
    )
    parser.add_argument(
        "--source",
        default=None if "source" not in kwargs else kwargs["source"],
        help="The ETL data source to extract, use the name specified in settings.",
    )
    parser.add_argument(
        "--dest",
        default=None if "dest" not in kwargs else kwargs["dest"],
        help=(
            "The place to load transformed data to, can be 'fs' or 'gcs'.\n"
            "Default is 'gcs', "
            "which the intermediate output will still write to 'fs'."
        ),
    )
    parser.add_argument(
        "--step",
        default=None if "step" not in kwargs else kwargs["step"],
        help=(
            "The ETL step to run to, "
            "can be 'extract', 'transform', 'load', or just the first letter. \n"
            "Default is 'load', which means go through the whole ETL process."
        ),
    )
    parser.add_argument(
        "--date",
        type=lambda x: datetime.datetime.strptime(x, DEFAULT_DATE_FORMAT),
        default=datetime.datetime.today() if "date" not in kwargs else kwargs["date"],
        help="The base (latest) date of the data in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--period",
        type=int,
        default=30 if "period" not in kwargs else kwargs["period"],
        help="Period of data in days.",
    )
    parser.add_argument(
        "--rm",
        default=False if "rm" not in kwargs else kwargs["rm"],
        action="store_true",
        help="Clean up cached files.",
    )
    return parser
