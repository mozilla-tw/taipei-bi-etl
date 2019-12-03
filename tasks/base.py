"""Task base."""
import errno
import glob
import inspect
import time
from argparse import Namespace
import os
import os.path
import requests
import datetime
from pandas import DataFrame
import pandas_gbq as pdbq
from google.cloud import storage
import numpy as np
from typing import List, Tuple, Union, Dict, Any
from pandas_schema import Column, Schema
from pandas_schema.validation import IsDtypeValidation
from utils.cache import check_extract_cache
from utils.config import DEFAULT_DATE_FORMAT, DEFAULT_DATETIME_FORMAT
from utils.file import (
    get_path_format,
    get_file_ext,
    get_path_prefix,
    read_string,
    write_string,
)
from utils.marshalling import lookback_dates, json_extract, convert_df, convert_format
from utils.query import build_query
import logging

log = logging.getLogger(__name__)


class EtlTask:
    """Base ETL task to serve common extract/load functions."""

    def __init__(
        self,
        args: Namespace,
        sources: Dict[str, Any],
        schema: List[Tuple[str, np.generic]],
        destinations: Dict[str, Any],
        stage: str,
        task: str,
    ):
        """Initiate parameters and client libraries for ETL task.

        :param args: args passed from command line,
        see `get_arg_parser()`
        :param sources: data source to be extracted,
        specified in task config, see `configs/*.py`
        :param schema: the target schema to load to.
        :param destinations: destinations to load data to,
        specified in task config, see `configs/*.py`
        :param stage: the stage of the loaded data, could be staging/production.
        :param task: the name of the task.
        """
        # Clear cached files
        if args.rm:
            for source in sources:
                files = []
                files += glob.glob(
                    get_path_format(True).format(
                        prefix=destinations["fs"]["prefix"],
                        stage="raw",
                        task=args.task,
                        source=source,
                    )
                )
                files += glob.glob(
                    get_path_format(True).format(
                        prefix=destinations["fs"]["prefix"],
                        stage=stage,
                        task=args.task,
                        source=source,
                    )
                )
                for f in files:
                    log.info("Removing cached file: %s" % f)
                    os.remove(f)
        self.task = task
        self.stage = stage
        self.args = args
        self.period = args.period
        self.current_date = args.date
        self.last_month = lookback_dates(args.date, args.period)
        self.sources = sources
        coltypes = []
        for coltype in schema:
            coltypes += [Column(coltype[0], [IsDtypeValidation(coltype[1])])]
        self.schema = Schema(coltypes)
        self.raw_schema = schema
        self.destinations = destinations
        self.raw = dict()
        self.extracted_base = dict()
        self.extracted = dict()
        self.transformed = dict()
        self.gcs = storage.Client()

    def get_filepaths(
        self,
        source: str,
        config: Dict[str, Any],
        stage: str,
        dest: str,
        date: datetime.datetime = None,
    ) -> List[str]:
        """Get existing data file paths with wildcard page number.

        :rtype: list[str]
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param stage: the stage of the loaded data, could be raw/staging/production.
        :param dest: name of the destination to load data to,
            specified in task config, see `configs/*.py`
        :param date: the date part of the data file name,
            will use `self.current_date` if not specified
        :return: a list of data file paths
        """
        if config["type"] == "gcs":
            if dest == "gcs":
                prefix = config["prefix"]
            else:
                prefix = self.destinations[dest]["prefix"]
            return glob.glob(prefix + config["path"] + config["filename"])
        else:
            return glob.glob(
                get_path_format().format(
                    stage=stage,
                    task=self.task,
                    source=source,
                    prefix=self.destinations[dest]["prefix"],
                    filename=self.get_filename(source, config, stage, dest, "*", date),
                )
            )

    def get_filepath(
        self,
        source: str,
        config: Dict[str, Any],
        stage: str,
        dest: str,
        page: Union[int, str] = None,
        date: datetime.datetime = None,
    ) -> str:
        """Get data file path.

        The format would be {prefix}{stage}-{task}-{source}/{filename}

        :rtype: str
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param stage: the stage of the loaded data, could be raw/staging/production.
        :param dest: name of the destination to load data to,
            specified in task config, see `configs/*.py`
        :param page: the page part of the data file name
        :param date: the date part of the data file name,
            will use `self.current_date` if not specified
        :return: the data file path
        """
        if config["type"] == "gcs":
            if dest == "gcs":
                prefix = config["prefix"]
            else:
                prefix = self.destinations[dest]["prefix"]
            fpath = prefix + config["path"] + config["filename"]
            if page is not None:
                fpath = fpath.replace("*", page)
            return fpath
        else:
            return get_path_format().format(
                stage=stage,
                task=self.task,
                source=source,
                prefix=self.destinations[dest]["prefix"],
                filename=self.get_filename(source, config, stage, dest, page, date),
            )

    def get_filename(
        self,
        source: str,
        config: Dict[str, Any],
        stage: str,
        dest: str,
        page: Union[int, str] = None,
        date: datetime.datetime = None,
    ) -> str:
        """Get data file name.

        The format would be {date}.{page}.{ext} for raw data,
        or {date}.{ext} otherwise.

        :rtype: str
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param stage: the stage of the loaded data, could be raw/staging/production.
        :param page: the page part of the data file name
        :param date: the date part of the data file name,
            will use `self.current_date` if not specified
        :return: the data file name
        """
        date = self.current_date if date is None else date
        if stage == "raw":
            if "iterator" in config:
                page = config["iterator"][0] if page is None else page
            ftype = "json" if "file_format" not in config else config["file_format"]
            return "{date}.{page}.{ext}".format(
                date=date.strftime(DEFAULT_DATE_FORMAT),
                ext=ftype,
                page=1 if page is None else page,
            )
        else:
            dest_config = self.destinations["fs"]
            ftype = (
                "jsonl"
                if "file_format" not in dest_config
                else dest_config["file_format"]
            )
            return "{date}.{ext}".format(
                date=date.strftime(DEFAULT_DATE_FORMAT), ext=ftype
            )

    def get_or_create_filepath(
        self,
        source: str,
        config: Dict[str, Any],
        stage: str,
        dest: str,
        page: Union[int, str] = None,
        date: datetime.datetime = None,
    ) -> str:
        """Get data file path for loading.

        The format would be {prefix}{stage}-{task}-{source}/{filename}.
        Folders will be created if doesn't exist.

        :rtype: str
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param stage: the stage of the loaded data, could be raw/staging/production.
        :param dest: name of the destination to load data to,
            specified in task config, see `configs/*.py`
        :param page: the page part of the data file name
        :param date: the date part of the data file name,
            will use `self.current_date` if not specified
        :return: the data file path
        """
        filename = self.get_filepath(source, config, stage, dest, page, date)
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        return filename

    def is_cached(
        self, source: str, config: Dict[str, Any], stage: str = "raw"
    ) -> bool:
        """Check whether a raw data is cached.

        Note that this currently only used for raw data extracted from API.

        :rtype: bool
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param stage: the stage of the data, could be raw/staging/production.
        :return: whether a data file is cached in local file system
        """
        fpath = self.get_filepath(source, config, stage, "fs")
        return os.path.isfile(fpath)

    def get_target_dataframe(
        self, schema: List[Tuple[str, np.generic]] = None
    ) -> DataFrame:
        """Get an empty DataFrame with target schema.

        :param schema: list of tuples(column name, numpy data type),
            see `configs/*.py`
        :rtype: DataFrame
        :return: an empty DataFrame with target schema
        """
        return DataFrame(
            np.empty(0, dtype=np.dtype(self.raw_schema if schema is None else schema))
        )

    def extract_via_fs(
        self,
        source: str,
        config: Dict[str, Any],
        stage: str = "raw",
        date: datetime.datetime = None,
    ) -> DataFrame:
        """Extract data from file system and convert into DataFrame.

        The logic is based on task config, see `configs/*.py`

        :rtype: DataFrame
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param stage: the stage of the loaded data, could be raw/staging/production.
        :param date: the date part of the data file name,
            will use `self.current_date` if not specified
        :return: the extracted DataFrame
        """
        # extract paged raw files
        if "paths" in config:
            fpaths = config["paths"]
        elif stage == "raw":
            fpaths = self.get_filepaths(source, config, stage, "fs", date)
        else:
            fpaths = [self.get_filepath(source, config, stage, "fs", date)]
        if "iterator" in config:
            extracted = None if "iterator" not in config else dict()
            for fpath in fpaths:
                raw = read_string(fpath)
                it = get_file_ext(fpath)
                self.raw[it] = raw
                extracted[it] = convert_df(raw, config)
            log.info(
                "%s-%s-%s/%s x %d iterators extracted from file system"
                % (
                    stage,
                    self.task,
                    source,
                    (self.current_date if date is None else date).date(),
                    len(fpaths),
                )
            )
        else:
            extracted = None
            for fpath in fpaths:
                raw = read_string(fpath)
                if extracted is None:
                    self.raw[source] = [raw]
                    extracted = convert_df(raw, config)
                else:
                    self.raw[source] += [raw]
                    extracted = extracted.append(convert_df(raw, config))
            extracted = extracted.reset_index(drop=True)
            log.info(
                "%s-%s-%s/%s x %d pages extracted from file system"
                % (
                    stage,
                    self.task,
                    source,
                    (self.current_date if date is None else date).date(),
                    len(fpaths),
                )
            )
        return extracted

    @check_extract_cache
    def extract_via_gcs(
        self,
        source: str,
        config: Dict[str, Any],
        stage: str = "raw",
        date: datetime.datetime = None,
    ) -> DataFrame:
        """Download blobs from Google Cloud Storage bucket and convert into DataFrame.

        The logic is based on task config, see `configs/*.py`

        :rtype: DataFrame
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param stage: the stage of the loaded data, could be raw/staging/production.
        :param date: the date part of the data file name,
            will use `self.current_date` if not specified
        :return: the extracted `DataFrame`
        """
        if config["type"] == "gcs":
            bucket = config["bucket"]
            prefix = self.get_filepath(source, config, stage, "gcs")
        else:
            # currently used to extract cached API data from GCS for validation
            bucket = self.destinations["gcs"]["bucket"]
            prefix = self.get_filepath(source, config, stage, "gcs", "*", date)
            prefix = get_path_prefix(prefix)
        blobs = self.gcs.list_blobs(bucket, prefix=prefix)

        i = 0
        is_empty = True
        for i, blob in enumerate(blobs):
            is_empty = False
            page = get_file_ext(blob.name)
            filepath = self.get_or_create_filepath(
                source, config, stage, "fs", page, date
            )
            blob.download_to_filename(filepath)

        if not is_empty:
            if config["type"] == "gcs":
                log.info(
                    "%s x %d pages extracted from google cloud storage"
                    % (prefix, i + 1)
                )
            else:
                log.info(
                    "%s-%s-%s/%s x %d pages extracted from google cloud storage"
                    % (
                        stage,
                        self.task,
                        source,
                        (self.current_date if date is None else date).date(),
                        i + 1,
                    )
                )
            return self.extract_via_fs(source, config, stage, date)
        else:
            return DataFrame()

    @check_extract_cache
    def extract_via_api(
        self,
        source: str,
        config: Dict[str, Any],
        stage: str = "raw",
        date: datetime.datetime = None,
    ) -> Union[DataFrame, Dict[str, DataFrame]]:
        """Extract data from API and convert into DataFrame.

        The logic is based on task config, see `configs/*.py`

        :rtype: DataFrame
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param stage: the stage of the loaded data, could be raw/staging/production.
        :param date: the date part of the data file name,
            will use `self.current_date` if not specified
        :return: the extracted `DataFrame`
        """
        # API paging
        start_date = (
            self.last_month.strftime(config["date_format"])
            if date is None
            else lookback_dates(date, self.period)
        )
        end_date = (
            self.current_date.strftime(config["date_format"]) if date is None else date
        )
        request_interval = (
            config["request_interval"] if "request_interval" in config else 1
        )
        if "iterator" in config:
            raw = dict()
            extracted = dict()
            for it in config["iterator"]:
                log.debug("waiting for %s iterator %d" % (source, it))
                time.sleep(request_interval)
                it = str(it)
                url = config["url"].format(
                    api_key=config["api_key"],
                    start_date=start_date,
                    end_date=end_date,
                    iterator=it,
                )
                r = requests.get(url, allow_redirects=True)
                raw[it] = r.text
                extracted[it] = convert_df(raw[it], config)
            self.raw[source] = raw
            log.info(
                "%s-%s-%s/%s x %d iterators extracted from API"
                % (stage, self.task, source, self.current_date.date(), len(extracted))
            )
            return extracted
        elif "page_size" in config:
            limit = config["page_size"]
            url = config["url"].format(
                api_key=config["api_key"],
                start_date=start_date,
                end_date=end_date,
                page=1,
                limit=limit,
            )
            r = requests.get(url, allow_redirects=True)
            raw = [r.text]
            extracted = convert_df(raw[0], config)
            count = int(json_extract(raw[0], config["json_path_page_count"]))
            if count is None or int(count) <= 1:
                self.raw[source] = raw
                log.info(
                    "%s-%s-%s/%s x 1 page extracted from API"
                    % (stage, self.task, source, self.current_date.date())
                )
                return extracted
            for page in range(2, count):
                log.debug("waiting for %s page %d" % (source, page))
                time.sleep(request_interval)
                url = config["url"].format(
                    api_key=config["api_key"],
                    start_date=start_date,
                    end_date=end_date,
                    page=page,
                    limit=limit,
                )
                r = requests.get(url, allow_redirects=True)
                raw += [r.text]
                extracted = extracted.append(convert_df(raw[page - 1], config))
            extracted = extracted.reset_index(drop=True)
            self.raw[source] = raw
            log.info(
                "%s-%s-%s/%s x %d pages extracted from API"
                % (stage, self.task, source, self.current_date.date(), count)
            )
            return extracted
        else:
            url = config["url"].format(
                api_key=config["api_key"], start_date=start_date, end_date=end_date
            )
            r = requests.get(url, allow_redirects=True)
            raw = r.text
            self.raw[source] = raw
            log.info(
                "%s-%s-%s/%s extracted from API"
                % ("raw", self.task, source, self.current_date.date())
            )
            return convert_df(raw, config)

    @check_extract_cache
    def extract_via_bq(
        self,
        source: str,
        config: Dict[str, Any],
        stage: str = "raw",
        date: datetime.datetime = None,
    ) -> DataFrame:
        """Extract data from Google BigQuery and convert into DataFrame.

        The logic is based on task config, see `configs/*.py`

        :rtype: DataFrame
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :return: the extracted `DataFrame`
        """
        query = build_query(
            config,
            self.last_month.strftime(config["date_format"]),
            self.current_date.strftime(config["date_format"]),
        )
        df = pdbq.read_gbq(query)
        self.raw[source] = convert_format(
            self.destinations["fs"]["file_format"],
            df,
            None if "date_fields" not in config else config["date_fields"],
        )
        log.info(
            "%s-%s-%s/%s w/t %d records extracted from BigQuery"
            % ("raw", self.task, source, self.current_date.date(), len(df.index))
        )
        return df

    @staticmethod
    def extract_via_const(source: str, config: Dict[str, Any]) -> DataFrame:
        """Extract data from Google BigQuery and convert into DataFrame.

        The logic is based on task config, see `configs/*.py`

        :rtype: DataFrame
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :return: the extracted `DataFrame`
        """
        return DataFrame(config["values"])

    def extract(self):
        """Extract data and convert into DataFrames based on settings.

        This will iterate through data source settings in task config
        (see `configs/*.py`)
        and extract them accordingly based on source type and the source argument.
        see also `get_arg_parser()`.

        """
        for source in self.sources:
            if not self.args.source or source in self.args.source.split(","):
                config = self.sources[source]
                if self.sources[source]["type"] == "api":
                    self.extracted[source] = self.extract_via_api(source, config)
                elif self.sources[source]["type"] == "gcs":
                    self.extracted[source] = self.extract_via_gcs(source, config)
                elif self.sources[source]["type"] == "bq":
                    self.extracted[source] = self.extract_via_bq(source, config)
                elif self.sources[source]["type"] == "file":
                    self.extracted[source] = self.extract_via_fs(source, config)
                elif self.sources[source]["type"] == "const":
                    self.extracted[source] = self.extract_via_const(source, config)

    def transform(self):
        """Transform extracted data into target format DataFrames.

        This will iterate through data source settings in task config
        (see `configs/*.py`)
        and transform extracted DataFrames accordingly
        based on the source argument (see also `get_arg_parser()`),
        will need to create a function for each data source when inheriting this class.
        e.g. `def transform_google_search(source, config)`
        for google_search data source, which the `source` represents the name of the
        data source to be extracted,
        `config` is the config of the data source to be extracted,
        both specified in task config, see `configs/*.py`
        """
        for source in self.sources:
            if not self.args.source or source in self.args.source.split(","):
                config = self.sources[source]
                # only transform data to be loaded
                if "load" in config and config["load"]:
                    assert self.extracted is not None
                    transform_method = getattr(self, "transform_{}".format(source))
                    transform_args = inspect.getfullargspec(transform_method).args
                    avail_args = {"source": source, "config": config, **self.extracted}
                    transform_kwargs = {}
                    for transform_arg in transform_args:
                        if transform_arg == "self":
                            continue
                        if transform_arg in avail_args:
                            # make DataFrame copies to avoid changing the extracted one
                            arg = (
                                avail_args[transform_arg].copy()
                                if isinstance(avail_args[transform_arg], DataFrame)
                                else avail_args[transform_arg]
                            )
                            transform_kwargs[transform_arg] = arg
                        else:
                            assert False, "Invalid transform arg %s" % transform_arg
                    self.transformed[source] = transform_method(**transform_kwargs)
                    errors = self.schema.validate(self.transformed[source])
                    error_msg = ""
                    for error in errors:
                        error_msg += error.message + "\n"
                    assert len(errors) == 0, error_msg
                    log.info(
                        "%s-%s-%s/%s w/t %d records transformed"
                        % (
                            self.stage,
                            self.task,
                            source,
                            self.current_date.date(),
                            len(self.transformed[source].index),
                        )
                    )

    def load_to_fs(self, source: str, config: Dict[str, Any], stage: str = "raw"):
        """Load data into file system based on destination settings.

        The logic is based on task config (see `configs/*.py`).

        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param stage: the stage of the loaded data, could be raw/staging/production.
        """
        if stage == "raw":
            fpath = self.get_or_create_filepath(source, config, stage, "fs")
            # write multiple raw files if paged
            raw = self.raw[source]
            if isinstance(raw, list):
                for i, r in enumerate(raw):
                    fpath = self.get_or_create_filepath(
                        source, config, stage, "fs", i + 1
                    )
                    write_string(fpath, r)
                log.info(
                    "%s-%s-%s/%s x %d pages loaded to file system."
                    % (stage, self.task, source, self.current_date.date(), len(raw))
                )
            elif isinstance(raw, dict):
                for i, r in raw.items():
                    fpath = self.get_or_create_filepath(source, config, stage, "fs", i)
                    write_string(fpath, r)
                log.info(
                    "%s-%s-%s/%s x %d pages loaded to file system."
                    % (stage, self.task, source, self.current_date.date(), len(raw))
                )
            else:
                write_string(fpath, raw)
                log.info(
                    "%s-%s-%s/%s x 1 page loaded to file system."
                    % (stage, self.task, source, self.current_date.date())
                )
        else:
            df = self.transformed[source]
            if "date_field" in self.destinations["fs"]:
                ds = df[self.destinations["fs"]["date_field"]].dt.date.unique()
                # load files by date
                for d in ds:
                    ddf = df[
                        (df[self.destinations["fs"]["date_field"]].dt.date == d)
                    ].copy()
                    # Fix date format for BigQuery (only support dash notation)
                    for rs in self.raw_schema:
                        if rs[1] == np.datetime64:
                            if 'datetime' in rs[0]:
                                ddf[rs[0]] = ddf[rs[0]].dt.strftime(DEFAULT_DATETIME_FORMAT)
                            else:
                                ddf[rs[0]] = ddf[rs[0]].dt.strftime(DEFAULT_DATE_FORMAT)
                    self.convert_file(ddf, config, source, stage, d)
                log.info(
                    "%s-%s-%s/%s x %d files loaded to file system."
                    % (stage, self.task, source, self.current_date.date(), len(ds))
                )
            else:
                self.convert_file(df, config, source, stage)
                log.info(
                    "%s-%s-%s/%s x 1 files loaded to file system."
                    % (stage, self.task, source, self.current_date.date())
                )

    def convert_file(
        self,
        df: DataFrame,
        config: Dict[str, Any],
        source: str,
        stage: str,
        date: datetime.datetime = None,
    ):
        """Convert DataFrame into destination files.

        The logic is based on task config (see `configs/*.py`).

        :param df: the DataFrame to convert
        :param config: the corresponding source config
        :param source: the name of the data source
        :param stage: the stage of the data
        :param date: the date of the data
        """
        date = self.current_date if date is None else date
        fpath = self.get_or_create_filepath(source, config, stage, "fs", None, date)
        output = convert_format(self.destinations["fs"]["file_format"], df)
        write_string(fpath, output)

    def load_to_gcs(self, source: str, config: Dict[str, Any], stage: str = "raw"):
        """Load data into Google Cloud Storage based on destination settings.

        The logic is based on task config (see `configs/*.py`).

        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param stage: the stage of the loaded data, could be raw/staging/production.
        """
        bucket = self.gcs.bucket(self.destinations["gcs"]["bucket"])
        if stage == "raw":
            fpaths = self.get_filepaths(source, config, stage, "fs")
            fl = len(fpaths)
            for fpath in fpaths:
                blob = bucket.blob(
                    self.get_filepath(source, config, stage, "gcs", get_file_ext(fpath))
                )
                blob.upload_from_filename(fpath)
        else:
            # load files by date
            df = self.transformed[source]

            if "date_field" in self.destinations["fs"]:
                ds = df[self.destinations["fs"]["date_field"]].dt.date.unique()
                fl = len(ds)
                for d in ds:
                    blob = bucket.blob(
                        self.get_filepath(source, config, stage, "gcs", None, d)
                    )
                    blob.upload_from_filename(
                        self.get_filepath(source, config, stage, "fs", None, d)
                    )
            else:
                fl = 1
                blob = bucket.blob(self.get_filepath(source, config, stage, "gcs"))
                blob.upload_from_filename(
                    self.get_filepath(source, config, stage, "fs")
                )
        log.info(
            "%s-%s-%s/%s x %d files loaded to GCS."
            % (stage, self.task, source, self.current_date.date(), fl)
        )

    def load(self):
        """Load transformed files into destinations.

        This will iterate through data source settings in task config
        (see `configs/*.py`)
        and load transformed data accordingly based on the destination argument,
        see also `get_arg_parser()`.

        """
        for source in self.sources:
            if not self.args.source or source in self.args.source.split(","):
                config = self.sources[source]
                if "load" in config and config["load"]:
                    assert self.transformed[source] is not None
                    self.load_to_fs(source, config, self.stage)
                    if self.args.dest != "fs":
                        self.load_to_gcs(source, config, self.stage)

    def run(self):
        """Run the whole ETL process based on the step argument.

        See `get_arg_parser()`.

        """
        if self.args.step and self.args.step[0].upper() not in ["E", "T", "L"]:
            raise ValueError("Invalid argument specified.")
        if not self.args.step or self.args.step[0].upper() in ["E", "T", "L"]:
            self.extract()
        if not self.args.step or self.args.step[0].upper() in ["T", "L"]:
            self.transform()
        if not self.args.step or self.args.step[0].upper() in ["L"]:
            self.load()
