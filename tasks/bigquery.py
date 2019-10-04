import datetime
import logging
from argparse import Namespace
from typing import Dict
import utils.config
from google.cloud import bigquery

from utils.file import read_string

log = logging.getLogger(__name__)

DEFAULTS = {}


class BqTask:
    def __init__(self, config: Dict, date: datetime.datetime):
        self.date = date.strftime(utils.config.DEFAULT_DATE_FORMAT)
        self.config = config
        self.client = bigquery.Client(config["project"])

    def create_schema(self):
        udfs = []
        if "udf" in self.config:
            udfs += [("udf_%s" % x, read_string("udf/{}.sql".format(x))) for x in self.config["udf"]]
        if "udf_js" in self.config:
            udfs += [("udf_js_%s" % x, read_string("udf_js/{}.sql".format(x))) for x in self.config["udf_js"]]
        for udf, qstring in udfs:
            qstring = qstring % (self.config["project"], self.config["dataset"])
            # Initiate the query to create the routine.
            query_job = self.client.query(qstring)  # Make an API request.
            # Wait for the query to complete.
            query_job.result()  # Waits for the job to complete.
            log.info("Created routine {}".format(query_job.ddl_target_routine))

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
    def __init__(self, config: Dict, date: datetime):
        super().__init__(config, date)

    def create_schema(self):
        self.daily_run()

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
    def __init__(self, config: Dict, date: datetime):
        super().__init__(config, date)

    def create_schema(self):
        # Run a empty query to create schema
        self.run_query('1970-01-01')

    def daily_run(self):
        self.run_query(self.date)

    def run_query(self, date):
        qstring = read_string("sql/{}.sql".format(self.config["query"]))
        table_ref = self.client.dataset(self.config["dataset"]).table(
            self.config["dest"]
        )
        job_config = bigquery.QueryJobConfig()
        job_config.write_disposition = self.config["write_disposition"] if "write_disposition" in self.config else bigquery.job.WriteDisposition.WRITE_APPEND
        job_config.destination = table_ref
        qparams = {
            **self.config,
            'start_date': date,
        }
        query = self.client.query(
            qstring.format(**qparams),
            job_config=job_config,
        )
        query.result()

# https://cloud.google.com/bigquery/docs/views
class BqViewTask(BqTask):
    def __init__(self, config: Dict, date: datetime.datetime):
        super().__init__(config, date)

    def create_schema(self):
        super().create_schema()
        qstring = read_string("sql/{}.sql".format(self.config["query"]))
        shared_dataset_ref = self.client.dataset(self.config["dataset"])
        view_ref = shared_dataset_ref.table(self.config["dest"])
        view = bigquery.Table(view_ref)
        qparams = {
            **self.config,
            'start_date': self.date,
        }
        view.view_query = qstring.format(**qparams)
        view = self.client.create_table(view)  # API request

        print("Successfully created view at {}".format(view.full_table_id))


def get_task_by_config(config: Dict, date: datetime.datetime):
    assert "type" in config, "Task type is required in BigQuery config."
    if config["type"] == "gcs":
        return BqGcsTask(config, date)
    elif config["type"] == "view":
        return BqViewTask(config, date)
    elif config["type"] == "table":
        return BqTableTask(config, date)


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
    events_task = get_task_by_config(configs.MANGO_EVENTS, args.date)
    unnested_events_task = get_task_by_config(configs.MANGO_EVENTS_UNNESTED, args.date)
    channel_mapping_task = get_task_by_config(configs.CHANNEL_MAPPING, args.date)
    user_channels_task = get_task_by_config(configs.USER_CHANNELS, args.date)
    if args.dropschema:
        events_task.drop_schema()
        unnested_events_task.drop_schema()
        channel_mapping_task.drop_schema()
        user_channels_task.drop_schema()
    if args.createschema:
        events_task.create_schema()
        unnested_events_task.create_schema()
        channel_mapping_task.create_schema()
        user_channels_task.create_schema()
    events_task.daily_run()
    channel_mapping_task.daily_run()


if __name__ == "__main__":
    arg_parser = utils.config.get_arg_parser(**DEFAULTS)
    main(arg_parser.parse_args())
