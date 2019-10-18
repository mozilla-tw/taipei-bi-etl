"""BigQuery Etl Tasks."""
import datetime
import logging
import re
from argparse import Namespace
from typing import Dict, Callable, Optional

from google.cloud.exceptions import NotFound

import utils.config
from google.cloud import bigquery

from utils.file import read_string

log = logging.getLogger(__name__)

DEFAULTS = {}
FILETYPES = {
    "csv": bigquery.SourceFormat.CSV,
    "jsonl": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
}


class BqTask:
    """Base class for BigQuery ETL."""

    def __init__(self, config: Dict, date: datetime.datetime):
        # default to load data 1 day behind
        self.date = (
            date
            - datetime.timedelta(
                days=1 if "days_behind" not in config else config["days_behind"]
            )
        ).strftime(utils.config.DEFAULT_DATE_FORMAT)
        self.config = config
        self.client = bigquery.Client(config["params"]["project"])

    def is_write_append(self):
        # default write append=true
        return "append" not in self.config or self.config["append"]

    def does_table_exist(self):
        try:
            dataset = self.client.dataset(self.config["params"]["dataset"])
            table_ref = dataset.table(self.config["params"]["dest"])
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False

    def does_routine_exist(self, routine_id):
        try:
            dataset = self.client.dataset(self.config["params"]["dataset"])
            routine_ref = dataset.routine(routine_id)
            self.client.get_routine(routine_ref)
            return True
        except NotFound:
            return False

    def create_schema(self, check_exists=False):
        udfs = []
        if "udf" in self.config:
            udfs += [
                ("udf_%s" % x, read_string("udf/{}.sql".format(x)))
                for x in self.config["udf"]
            ]
        if "udf_js" in self.config:
            udfs += [
                ("udf_js_%s" % x, read_string("udf_js/{}.sql".format(x)))
                for x in self.config["udf_js"]
            ]
        for udf, qstring in udfs:
            if check_exists and self.does_routine_exist(udf):
                continue
            qstring = qstring % (
                self.config["params"]["project"],
                self.config["params"]["dataset"],
            )
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
            self.client.delete_routine(
                "%s.%s.%s"
                % (
                    self.config["params"]["project"],
                    self.config["params"]["dataset"],
                    udf,
                ),
                not_found_ok=True,
            )
        self.client.delete_table(
            "%s.%s.%s"
            % (
                self.config["params"]["project"],
                self.config["params"]["dataset"],
                self.config["params"]["dest"],
            ),
            not_found_ok=True,
        )
        log.info("Deleted table '{}'.".format(self.config["params"]["dest"]))

    def daily_run(self):
        assert False, "daily_run not implemented."

    def daily_cleanup(self, d):
        if self.is_write_append() and "cleanup_query" in self.config:
            qstring = read_string("sql/{}.sql".format(self.config["cleanup_query"]))
            qparams = self.get_query_params(d)
            query_job = self.client.query(qstring.format(**qparams))
            query_job.result()
            log.info("Done cleaning up.")

    def get_query_params(self, d):
        return {**self.config["params"], "start_date": d}


# https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json
class BqGcsTask(BqTask):
    """BigQuery ETL via GCS."""

    def __init__(self, config: Dict, date: datetime):
        super().__init__(config, date)

    def create_schema(self, check_exists=False):
        if check_exists and self.does_table_exist():
            return
        # load a file to create schema
        self.run_query(self.date, True)

    def daily_run(self):
        self.daily_cleanup(self.date)
        self.run_query(self.date)

    def run_query(self, date, autodetect=False):
        dataset_ref = self.client.dataset(self.config["params"]["dataset"])
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = (
            bigquery.WriteDisposition.WRITE_APPEND
            if self.is_write_append()
            else bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        # don't do autodetect after schema created, may have errors on STRING/INTEGER
        job_config.autodetect = autodetect
        job_config.source_format = FILETYPES[self.config["filetype"]]
        if "partition_field" in self.config:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=self.config["partition_field"],
            )
        uri = "gs://%s" % self.config["params"]["src"].format(start_date=date)

        load_job = self.client.load_table_from_uri(
            uri,
            dataset_ref.table(self.config["params"]["dest"]),
            location=self.config["params"]["location"],
            job_config=job_config,
        )
        log.info("Starting job {}".format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        log.info("Job finished.")

        destination_table = self.client.get_table(
            dataset_ref.table(self.config["params"]["dest"])
        )
        log.info("Loaded {} rows.".format(destination_table.num_rows))


# https://cloud.google.com/bigquery/docs/tables
# https://cloud.google.com/bigquery/docs/writing-results
class BqQueryTask(BqTask):
    """BigQuery ETL via query result."""

    def __init__(self, config: Dict, date: datetime):
        super().__init__(config, date)

    def create_schema(self, check_exists=False):
        super().create_schema(check_exists)
        if check_exists and self.does_table_exist():
            return
        start_date = "1970-01-01"
        if "init_query" in self.config:
            qstring = read_string("sql/{}.sql".format(self.config["init_query"]))
            qparams = self.get_query_params(start_date)
            qstring = qstring.format(**qparams)
            self.run_query(start_date, qstring)
        else:
            # Run a empty query to create schema
            qstring = read_string("sql/{}.sql".format(self.config["query"]))
            qparams = self.get_query_params(start_date)
            qstring = qstring.format(**qparams)
            LIMIT_REGEX = r"(.*)(LIMIT\s+[0-9]+)(.*)"
            if re.match(LIMIT_REGEX, qstring, re.IGNORECASE):
                re.sub(LIMIT_REGEX, r"\1 LIMIT 0 \3", qstring, flags=re.IGNORECASE)
            else:
                qstring += " LIMIT 0"
            self.run_query(start_date, qstring)

    def daily_run(self):
        self.daily_cleanup(self.date)
        self.run_query(self.date)

    def run_query(self, date, qstring=None):
        if qstring is None:
            qstring = read_string("sql/{}.sql".format(self.config["query"]))
            qparams = self.get_query_params(date)
            qstring = qstring.format(**qparams)
        table_ref = self.client.dataset(self.config["params"]["dataset"]).table(
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

        query = self.client.query(qstring, job_config=job_config)
        query.result()


# https://cloud.google.com/bigquery/docs/views
class BqViewTask(BqTask):
    """BigQuery ETL via view."""

    def __init__(self, config: Dict, date: datetime.datetime):
        super().__init__(config, date - datetime.timedelta(days=1))

    def create_schema(self, check_exists=False):
        super().create_schema(check_exists)
        if check_exists and self.does_table_exist():
            return
        qstring = read_string("sql/{}.sql".format(self.config["query"]))
        shared_dataset_ref = self.client.dataset(self.config["params"]["dataset"])
        view_ref = shared_dataset_ref.table(self.config["params"]["dest"])
        view = bigquery.Table(view_ref)
        qparams = self.get_query_params(self.date)
        view.view_query = qstring.format(**qparams)
        view = self.client.create_table(view)  # API request

        log.info("Successfully created view at {}".format(view.full_table_id))


def get_task(config: Dict, date: datetime.datetime):
    assert "type" in config, "Task type is required in BigQuery config."
    if config["type"] == "gcs":
        return BqGcsTask(config, date)
    elif config["type"] == "view":
        return BqViewTask(config, date)
    elif config["type"] == "table":
        return BqQueryTask(config, date)


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
    if args.subtask:
        log.info("Running BigQuery Task %s." % args.subtask)
        cfg = getattr(cfgs, args.subtask.upper())
        task = get_task(cfg, args.date)
        if args.dropschema:
            task.drop_schema()
        if args.createschema:
            task.create_schema(args.checkschema)
        task.daily_run()
        log.info("BigQuery Task %s Finished." % args.subtask)
    # init(args, cfgs)
    # daily_run_lastest(args.date, cfgs)
    # backfill("2019-09-01", "2019-10-17", cfgs)
    # backfill("2019-09-01", "2019-09-02", cfgs)


def backfill(start, end, configs: Optional[Callable]):
    for d in get_date_range_from_string(start, end):
        daily_run(d, configs)


def daily_run_lastest(d: datetime, configs: Optional[Callable]):
    channel_mapping_task = get_task(configs.CHANNEL_MAPPING, d)
    channel_mapping_task.daily_run()
    daily_run(d, configs)
    backfill_dates = get_date_range(
        datetime.datetime.utcnow() - datetime.timedelta(days=8),
        datetime.datetime.utcnow() - datetime.timedelta(days=1),
    )
    for d in backfill_dates:
        revenue_bukalapak_task = get_task(configs.REVENUE_BUKALAPAK, d)
        revenue_bukalapak_task.daily_run()


def daily_run(d: datetime, configs: Optional[Callable]):
    print(d)
    core_task = get_task(configs.MANGO_CORE, d)
    core_task.daily_run()
    events_task = get_task(configs.MANGO_EVENTS, d)
    events_task.daily_run()
    # revenue_bukalapak_task = get_task(configs.REVENUE_BUKALAPAK, d)
    # revenue_bukalapak_task.daily_run()
    # feature_first_occur_task = get_task(configs.FEATURE_FIRST_OCCUR, d)
    # feature_first_occur_task.daily_run()


def init(args, configs: Optional[Callable]):
    core_task = get_task(configs.MANGO_CORE, args.date)
    events_task = get_task(configs.MANGO_EVENTS, args.date)
    unnested_events_task = get_task(configs.MANGO_EVENTS_UNNESTED, args.date)
    feature_events_task = get_task(configs.MANGO_EVENTS_FEATURE_MAPPING, args.date)
    google_rps_task = get_task(configs.GOOGLE_RPS, datetime.datetime(2018, 1, 1))
    channel_mapping_task = get_task(configs.CHANNEL_MAPPING, args.date)
    user_channels_task = get_task(configs.USER_CHANNELS, args.date)
    revenue_bukalapak_task = get_task(configs.REVENUE_BUKALAPAK, args.date)
    feature_first_occur_task = get_task(configs.FEATURE_FIRST_OCCUR, args.date)
    if args.dropschema:
        core_task.drop_schema()
        events_task.drop_schema()
        unnested_events_task.drop_schema()
        feature_events_task.drop_schema()
        channel_mapping_task.drop_schema()
        user_channels_task.drop_schema()
        google_rps_task.drop_schema()
        revenue_bukalapak_task.drop_schema()
        feature_first_occur_task.drop_schema()
    if args.createschema:
        core_task.create_schema(args.checkschema)
        events_task.create_schema(args.checkschema)
        unnested_events_task.create_schema(args.checkschema)
        feature_events_task.create_schema(args.checkschema)
        channel_mapping_task.create_schema(args.checkschema)
        user_channels_task.create_schema(args.checkschema)
        google_rps_task.create_schema(args.checkschema)
        revenue_bukalapak_task.create_schema(args.checkschema)
        feature_first_occur_task.create_schema(args.checkschema)


def get_date_range_from_string(start: str, end: str):
    starttime = datetime.datetime.strptime(start, utils.config.DEFAULT_DATE_FORMAT)
    endtime = datetime.datetime.strptime(end, utils.config.DEFAULT_DATE_FORMAT)
    return get_date_range(starttime, endtime)


def get_date_range(starttime, endtime):
    return [
        starttime + datetime.timedelta(days=x)
        for x in range(0, (endtime - starttime).days)
    ]


if __name__ == "__main__":
    arg_parser = utils.config.get_arg_parser(**DEFAULTS)
    main(arg_parser.parse_args())
