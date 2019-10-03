import logging
from argparse import Namespace
from typing import Dict
import utils.config
from google.cloud import bigquery

log = logging.getLogger(__name__)

DEFAULTS = {}

try:
    import importlib.resources as pkg_resources
except ImportError:
    # Try backported to PY<37 `importlib_resources`.
    import importlib_resources as pkg_resources


class BqTask:
    def __init__(self, config: Dict):
        self.config = config
        self.client = bigquery.Client(config["project"])

    def create_schema(self):
        assert False, "create_schema not implemented."

    def drop_schema(self):
        assert False, "drop_schema not implemented."

    def daily_run(self):
        assert False, "daily_run not implemented."


# https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json
class BqGcsTask(BqTask):
    def __init__(self, config: Dict):
        super().__init__(config)

    def drop_schema(self):
        pass

    def daily_run(self):
        dataset_ref = self.client.dataset(self.config["dataset"])
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.autodetect = True
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        uri = "gs://%s" % self.config["src"]

        load_job = self.client.load_table_from_uri(
            uri,
            dataset_ref.table(self.config["dest"]),
            location=self.config["location"],
            job_config=job_config,
        )
        log.info("Starting job {}".format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        log.info("Job finished.")

        destination_table = self.client.get_table(dataset_ref.table(self.config["dest"]))
        log.info("Loaded {} rows.".format(destination_table.num_rows))


# https://cloud.google.com/bigquery/docs/tables
# https://cloud.google.com/bigquery/docs/writing-results
class BqTableTask(BqTask):
    def __init__(self, config: Dict):
        super().__init__(config)

    def drop_schema(self):
        pass

    def daily_run(self, date="2019-09-28"):
        import sql

        queryString = pkg_resources.read_text(sql, self.config["sql_file"])
        table_ref = self.client.dataset(self.config["dataset"]).table(
            self.config["dest"]
        )
        job_config = bigquery.QueryJobConfig()
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND
        job_config.destination = table_ref

        query = self.client.query(
            queryString.format(src=self.config["src"], start_date=date),
            job_config=job_config,
        )
        query.result()


# https://cloud.google.com/bigquery/docs/views
class BqViewTask(BqTask):
    def __init__(self, config: Dict):
        super().__init__(config)

    def create_schema(self):
        pass

    def drop_schema(self):
        pass


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
    events_task = get_task_by_config(configs.MANGO_EVENTS)
    unnested_events_task = get_task_by_config(configs.MANGO_EVENTS_UNNESTED)
    channel_mapping_task = get_task_by_config(configs.CHANNEL_MAPPING)
    user_channels_task = get_task_by_config(configs.USER_CHANNELS)
    if args.dropschema:
        events_task.drop_schema()
        unnested_events_task.drop_schema()
        channel_mapping_task.drop_schema()
        user_channels_task.drop_schema()
    if args.createschema:
        unnested_events_task.create_schema()
        user_channels_task.create_schema()
    events_task.daily_run()
    channel_mapping_task.daily_run()


if __name__ == "__main__":
    arg_parser = utils.config.get_arg_parser(**DEFAULTS)
    main(arg_parser.parse_args())
