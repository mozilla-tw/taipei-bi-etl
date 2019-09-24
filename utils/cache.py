"""Cache utilities."""
import datetime
import functools
from typing import Dict, Any, Callable
from pandas import DataFrame
import logging

log = logging.getLogger(__name__)


def check_extract_cache(extract_func: Callable):
    """Return cached extracted results when cache hit.

    :param extract_func: the extract function to decorate
    :return: the decorated extract function
    """
    # decorator function definition
    @functools.wraps(extract_func)
    def cached_extract_wrapper(
        self,
        source: str,
        config: Dict[str, Any],
        stage: str = "raw",
        date: datetime.datetime = None,
    ) -> DataFrame:
        if "cache_file" in config and config["cache_file"]:
            if not self.is_cached(source, config):
                extracted = extract_func(self, source, config, stage, date)
                self.load_to_fs(source, config)
                if self.args.dest != "fs" and config["type"] == "api":
                    # load API cache to GCS as backup,
                    # not needed for BQ/GCS type since they are already on GCS
                    self.load_to_gcs(source, config)
            else:
                log.info(
                    "Cache hit for %s-%s-%s/%s"
                    % (
                        stage,
                        self.task,
                        source,
                        (self.current_date if date is None else date).date(),
                    )
                )
                # TODO: support different cache server when needed
                # e.g. memcached, redis, cloud memorystore, etc...
                extracted = self.extract_via_fs(source, config)
        else:
            extracted = extract_func(self, source, config, stage, date)
            if (
                self.args.dest != "fs"
                and config["type"] == "api"
                and "force_load_cache" in config
                and config["force_load_cache"]
            ):
                # force loading newly cached file to GCS,
                # by default API cache will only load to GCS on first call
                self.load_to_gcs(source, config)
        # Extract data from previous date for validation
        return extracted

    return cached_extract_wrapper
