import datetime
import logging
from argparse import Namespace
from typing import Dict, Callable, Optional
import utils.config
from google.cloud import bigquery

from utils.file import read_string

log = logging.getLogger(__name__)

DEFAULTS = {}


class BqTask:
    def __init__(self, config: Dict, date: datetime.datetime):
        self.date = (date - datetime.timedelta(days=1)).strftime(utils.config.DEFAULT_DATE_FORMAT)
        self.config = config
        self.client = bigquery.Client(config["id"]["project"])

    def is_write_append(self):
        # default write append=true
        return "append" not in self.config or self.config["append"]

    def create_schema(self):
        udfs = []
        if "udf" in self.config:
            udfs += [("udf_%s" % x, read_string("udf/{}.sql".format(x))) for x in self.config["udf"]]
        if "udf_js" in self.config:
            udfs += [("udf_js_%s" % x, read_string("udf_js/{}.sql".format(x))) for x in self.config["udf_js"]]
        for udf, qstring in udfs:
            qstring = qstring % (self.config["id"]["project"], self.config["id"]["dataset"])
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
            self.client.delete_routine("%s.%s.%s" % (self.config["id"]["project"], self.config["id"]["dataset"], udf), not_found_ok=True)
        self.client.delete_table("%s.%s.%s" % (self.config["id"]["project"], self.config["id"]["dataset"], self.config["params"]["dest"]), not_found_ok=True)
        log.info("Deleted table '{}'.".format(self.config["params"]["dest"]))

    def daily_run(self):
        assert False, "daily_run not implemented."


# https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json
class BqGcsTask(BqTask):
    def __init__(self, config: Dict, date: datetime):
        super().__init__(config, date)

    def create_schema(self):
        self.daily_run()

    def daily_run(self):
        dataset_ref = self.client.dataset(self.config["id"]["dataset"])
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND if self.is_write_append() else bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.autodetect = True
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        if "partition_field" in self.config:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=self.config["partition_field"],
            )
        uri = "gs://%s" % self.config["params"]["src"]

        load_job = self.client.load_table_from_uri(
            uri,
            dataset_ref.table(self.config["params"]["dest"]),
            location=self.config["id"]["location"],
            job_config=job_config,
        )
        log.info("Starting job {}".format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        log.info("Job finished.")

        destination_table = self.client.get_table(dataset_ref.table(self.config["params"]["dest"]))
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
        table_ref = self.client.dataset(self.config["id"]["dataset"]).table(
            self.config["params"]["dest"]
        )
        job_config = bigquery.QueryJobConfig()
        job_config.write_disposition = (
            bigquery.WriteDisposition.WRITE_APPEND
            if self.is_write_append()
            else bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        job_config.destination = table_ref
        if "partition_field" in self.config:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=self.config["partition_field"],
            )
        qparams = {
            **self.config["id"],
            **self.config["params"],
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
        shared_dataset_ref = self.client.dataset(self.config["id"]["dataset"])
        view_ref = shared_dataset_ref.table(self.config["params"]["dest"])
        view = bigquery.Table(view_ref)
        qparams = {
            **self.config["id"],
            **self.config["params"],
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
    cfgs = utils.config.get_configs("bigquery", config_name)
    init(args, cfgs)
    # daily_run(args.date, cfgs)
    backfill('2019-09-01', '2019-10-03', cfgs)


def backfill(start, end, configs: Optional[Callable]):
    for d in get_date_range(start, end):
        daily_run(d, configs)


def daily_run(d: datetime, configs: Optional[Callable]):
    print(d)
    core_task = get_task_by_config(configs.MANGO_CORE, d)
    core_task.daily_run()
    events_task = get_task_by_config(configs.MANGO_EVENTS, d)
    events_task.daily_run()


def init(args, configs: Optional[Callable]):
    core_task = get_task_by_config(configs.MANGO_CORE, args.date)
    events_task = get_task_by_config(configs.MANGO_EVENTS, args.date)
    unnested_events_task = get_task_by_config(configs.MANGO_EVENTS_UNNESTED, args.date)
    feature_events_task = get_task_by_config(configs.MANGO_EVENTS_FEATURE_MAPPING, args.date)
    channel_mapping_task = get_task_by_config(configs.CHANNEL_MAPPING, args.date)
    user_channels_task = get_task_by_config(configs.USER_CHANNELS, args.date)
    if args.dropschema:
        core_task.drop_schema()
        events_task.drop_schema()
        unnested_events_task.drop_schema()
        feature_events_task.drop_schema()
        channel_mapping_task.drop_schema()
        user_channels_task.drop_schema()
    if args.createschema:
        core_task.create_schema()
        events_task.create_schema()
        unnested_events_task.create_schema()
        feature_events_task.create_schema()
        channel_mapping_task.create_schema()
        user_channels_task.create_schema()


def get_date_range(start: str, end: str):
    starttime = datetime.datetime.strptime(start, utils.config.DEFAULT_DATE_FORMAT)
    endtime = datetime.datetime.strptime(end, utils.config.DEFAULT_DATE_FORMAT)
    return [starttime + datetime.timedelta(days=x) for x in range(0, (endtime - starttime).days)]


if __name__ == "__main__":
    arg_parser = utils.config.get_arg_parser(**DEFAULTS)
    main(arg_parser.parse_args())
