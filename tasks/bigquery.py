import logging
from argparse import Namespace
from typing import Dict

from google.cloud.bigquery import Routine, RoutineReference, RoutineArgument
from google.cloud.bigquery_v2.proto.standard_sql_pb2 import StandardSqlDataType

import utils.config
from google.cloud import bigquery

from utils.file import read_string
from utils.query import parse_udf

log = logging.getLogger(__name__)

DEFAULTS = {}


class BqTask:
    def __init__(self, config: Dict):
        self.config = config
        self.client = bigquery.Client(config["project"])

    def create_schema(self):
        pass

    def drop_schema(self):
        udfs = []
        if "udf" in self.config:
            udfs += ["udf_%s" % x for x in self.config["udf"]]
        if "udf_js" in self.config:
            udfs += ["udf_js_%s" % x for x in self.config["udf_js"]]
        for udf in udfs:
            self.client.delete_routine("%s.%s.%s" % (self.config["project"], self.config["dataset"], udf), not_found_ok=True)
        self.client.delete_table("%s.%s.%s" % (self.config["project"], self.config["dataset"], self.config["dest"]), not_found_ok=True)
        log.info("Deleted table '{}'.".format(self.config["dest"]))

    def daily_run(self):
        assert False, "daily_run not implemented."


# https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json
class BqGcsTask(BqTask):
    def __init__(self, config: Dict):
        super().__init__(config)

    def create_schema(self):
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

    def daily_run(self):
        pass


# https://cloud.google.com/bigquery/docs/views
class BqViewTask(BqTask):
    def __init__(self, config: Dict):
        super().__init__(config)

    def create_schema(self):
        super().create_schema()


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
