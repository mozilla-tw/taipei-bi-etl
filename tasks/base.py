"""Task base."""
import errno
import glob
import importlib
import inspect
import itertools
import re
import time
from io import StringIO
from argparse import Namespace
import os
import os.path
import requests
import datetime
import pandas as pd
from pandas import DataFrame, Series
import pandas_gbq as pdbq
from google.cloud import storage
import json
import numpy as np
import pandas.io.json as pd_json
from typing import List, Tuple, Union, Dict, Any, Callable
from pandas_schema import Column, Schema
from pandas_schema.validation import IsDtypeValidation
import pytz
import logging
from configs import mapping
from utils.config import (
    DEFAULT_DATE_FORMAT,
    DEFAULT_DATETIME_FORMAT,
    EXT_REGEX,
    DEFAULT_PATH_FORMAT,
    INJECTED_MAPPINGS,
)
from utils.marshalling import (
    flatten_dict,
    is_camel,
    decamelize,
    lookback_dates,
    json_extract,
    get_country_tz,
    get_tz_str,
    json_unnest,
)

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
                    EtlTask.get_path_format(True).format(
                        prefix=destinations["fs"]["prefix"],
                        stage="raw",
                        task=args.task,
                        source=source,
                    )
                )
                files += glob.glob(
                    EtlTask.get_path_format(True).format(
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

    @staticmethod
    def convert_df(raw: str, config: Dict[str, Any]) -> DataFrame:
        """Convert raw string to DataFrame, currently only supports json/csv.

        :rtype: DataFrame
        :param raw: the raw source string in json/csv format,
            this is to be converted to DataFrame
        :param config: the config of the data source specified in task config,
            see `configs/*.py`
        :return: the converted `DataFrame`
        """
        ftype = "json" if "file_format" not in config else config["file_format"]
        df = None
        if ftype == "jsonl":
            jlines = raw.split("\n")
            df = DataFrame()
            for jline in jlines:
                if len(jline) < 3:
                    continue
                line = json.loads(jline)
                df = df.append(Series(line), ignore_index=True)
        elif ftype == "json":
            if "json_path" in config:
                extracted_json = json_extract(raw, config["json_path"])
            elif "json_path_nested" in config:
                extracted_json = json_unnest(
                    raw, config["json_path_nested"], config["fields"], {}, []
                )
            else:
                extracted_json = raw
            data = pd_json.loads(extracted_json)
            df = pd_json.json_normalize(data)
        elif ftype == "csv":
            if "header" in config:
                df = pd.read_csv(StringIO(raw), names=config["header"])
            else:
                df = pd.read_csv(StringIO(raw))
        # convert timezone according to config
        tz = None
        if "timezone" in config:
            tz = pytz.timezone(config["timezone"])
        elif "country_code" in config:
            tz = get_country_tz(config["country_code"])
        # TODO: support multiple countries/timezones in the future if needed
        if "date_fields" in config:
            for date_field in config["date_fields"]:
                df[date_field] = pd.to_datetime(df[date_field])
            if tz is not None:
                df["tz"] = get_tz_str(tz)
                for date_field in config["date_fields"]:
                    df[date_field] = (
                        df[date_field].dt.tz_localize(tz).dt.tz_convert(pytz.utc)
                    )
                    df[date_field] = df[date_field].astype("datetime64[ns]")
        return df

    @staticmethod
    def get_file_ext(fpath: str) -> str:
        """Extract file extension from path.

        :rtype: str
        :param fpath: the path to extract
        :return: the extracted file extension
        """
        return re.search(EXT_REGEX, fpath).group(1)

    @staticmethod
    def get_prefix(fpath: str) -> str:
        """Extract prefix from path.

        :rtype: str
        :param fpath: the path to extract
        :return: the extracted prefix
        """
        ext_search = re.search(EXT_REGEX, fpath)
        return fpath[: ext_search.start()]

    @staticmethod
    def get_path_format(wildcard: bool = False) -> str:
        """Get the format string of file paths.

        :rtype: str
        :param wildcard: whether it's a wildcard path or not.
        :return: the path format string
        """
        if wildcard:
            return DEFAULT_PATH_FORMAT + "/*"
        else:
            return DEFAULT_PATH_FORMAT + "/{filename}"

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
                EtlTask.get_path_format().format(
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
            return EtlTask.get_path_format().format(
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
        if stage == "raw":
            fpaths = self.get_filepaths(source, config, stage, "fs", date)
        else:
            fpaths = [self.get_filepath(source, config, stage, "fs", date)]
        if "iterator" in config:
            extracted = None if "iterator" not in config else dict()
            for fpath in fpaths:
                with open(fpath, "r") as f:
                    raw = f.read()
                    it = EtlTask.get_file_ext(fpath)
                    self.raw[it] = raw
                    extracted[it] = self.convert_df(raw, config)
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
                with open(fpath, "r") as f:
                    raw = f.read()
                    if extracted is None:
                        self.raw[source] = [raw]
                        extracted = self.convert_df(raw, config)
                    else:
                        self.raw[source] += [raw]
                        extracted = extracted.append(self.convert_df(raw, config))
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
            # get cached file if already exists
            if self.is_cached(source, config, "staging"):
                return self.extract_via_fs(source, config, stage, date)
            bucket = config["bucket"]
            prefix = self.get_filepath(source, config, stage, "gcs")
        else:
            bucket = self.destinations["gcs"]["bucket"]
            prefix = self.get_filepath(source, config, stage, "gcs", "*", date)
            prefix = EtlTask.get_prefix(prefix)
        blobs = self.gcs.list_blobs(bucket, prefix=prefix)

        i = 0
        is_empty = True
        for i, blob in enumerate(blobs):
            is_empty = False
            page = EtlTask.get_file_ext(blob.name)
            filepath = self.get_filepath(source, config, stage, "fs", page, date)
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
                extracted[it] = self.convert_df(raw[it], config)
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
            extracted = self.convert_df(raw[0], config)
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
                extracted = extracted.append(self.convert_df(raw[page - 1], config))
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
            return self.convert_df(raw, config)

    def extract_via_api_or_cache(
        self,
        source: str,
        config: Dict[str, Any],
        stage: str = "raw",
        date: datetime.datetime = None,
    ) -> Tuple[DataFrame, DataFrame]:
        """Extract data from API and convert into DataFrame.

        The logic is based on task config, see `configs/*.py`

        :rtype: tuple(DataFrame, DataFrame)
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param stage: the stage of the loaded data, could be raw/staging/production.
        :param date: the date part of the data file name,
            will use `self.current_date` if not specified
        :return: the extracted `DataFrame` and another base DataFrame for validation
        """
        # use file cache to prevent calling partner API too many times
        if "cache_file" in config and config["cache_file"]:
            if not self.is_cached(source, config):
                extracted = self.extract_via_api(source, config, stage, date)
                self.load_to_fs(source, config)
                if self.args.dest != "fs":
                    self.load_to_gcs(source, config)
            else:
                extracted = self.extract_via_fs(source, config)
        else:
            extracted = self.extract_via_api(source, config, stage, date)
            if (
                self.args.dest != "fs"
                and "force_load_cache" in config
                and config["force_load_cache"]
            ):
                self.load_to_gcs(source, config)
        # Extract data from previous date for validation
        yesterday = lookback_dates(self.current_date, 1)
        if self.args.dest != "fs":
            extracted_base = self.extract_via_gcs(source, config, "raw", yesterday)
        else:
            extracted_base = self.extract_via_fs(source, config, "raw", yesterday)
        return extracted, extracted_base

    def extract_via_bq(self, source: str, config: Dict[str, Any]) -> DataFrame:
        """Extract data from Google BigQuery and convert into DataFrame.

        The logic is based on task config, see `configs/*.py`

        :rtype: DataFrame
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :return: the extracted `DataFrame`
        """
        query = self.build_query(
            config,
            self.last_month.strftime(config["date_format"]),
            self.current_date.strftime(config["date_format"]),
        )
        if "cache_file" in config and config["cache_file"]:
            if self.is_cached(source, config):
                return self.extract_via_fs(source, config)
            else:
                df = pdbq.read_gbq(query)
                self.raw[source] = self.convert_format(
                    df, None if "date_fields" not in config else config["date_fields"]
                )
                self.load_to_fs(source, config)
                if self.args.dest != "fs":
                    self.load_to_gcs(source, config)
                log.info(
                    "%s-%s-%s/%s w/t %d records extracted from BigQuery"
                    % (
                        "raw",
                        self.task,
                        source,
                        self.current_date.date(),
                        len(df.index),
                    )
                )
                return df
        else:
            df = pdbq.read_gbq(query)
            log.info(
                "%s-%s-%s/%s w/t %d records extracted from BigQuery"
                % ("raw", self.task, source, self.current_date.date(), len(df.index))
            )
            return df

    @staticmethod
    def build_query(config: Dict[str, Any], start_date: str, end_date: str) -> str:
        """Build query based on configs and args.

        :rtype: str
        :param config: the config of the query
        :param start_date: the start date string for the query
        :param end_date: the end date string for the query
        :return: the composed query string
        """
        query = ""
        if "udf" in config:
            for udf in config["udf"]:
                with open("udf/{}.sql".format(udf)) as f:
                    query += f.read()
        if "udf_js" in config:
            for udf_js in config["udf_js"]:
                with open("udf_js/{}.sql".format(udf_js)) as f:
                    query += f.read()
        if "query" in config:
            with open("sql/{}.sql".format(config["query"])) as f:
                query += f.read().format(
                    project=config["project"],
                    dataset=config["dataset"],
                    table=config["table"],
                    start_date=start_date,
                    end_date=end_date,
                )
        return query

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
                    (
                        self.extracted[source],
                        self.extracted_base[source],
                    ) = self.extract_via_api_or_cache(source, config)
                elif self.sources[source]["type"] == "gcs":
                    self.extracted[source] = self.extract_via_gcs(source, config)
                elif self.sources[source]["type"] == "bq":
                    self.extracted[source] = self.extract_via_bq(source, config)
                elif self.sources[source]["type"] == "const":
                    self.extracted[source] = self.extract_via_const(source, config)
                if "inject_mapping" in config:
                    INJECTED_MAPPINGS[config["inject_mapping"]] = self.extracted[source]
                self.extracted[source] = self.map_apply(config, self.extracted[source])

    @staticmethod
    def map_apply(config: Dict[str, Any], df: DataFrame) -> DataFrame:
        """Apply map functions to extracted DataFrame.

        :param config: the source config to check mapping settings
        :param df: the extracted DataFrame to be applied
        """
        maps = EtlTask.import_map_funcs(config)
        # do nothing if no map functions found in config
        if not maps:
            return df
        # handle map product (e.g. feature x channel)
        map_list = []
        for map_name, map_types in maps.items():
            ls = []
            for t, map_funcs in map_types.items():
                ls += [(map_name, t, map_funcs)]
            map_list += [ls]
        map_prod = itertools.product(*map_list)
        output = DataFrame()
        # apply maps here
        for batch in map_prod:
            # use a clean DataFrame for each batch
            d = df.copy()
            for map_name, map_type, map_funcs in batch:
                for idx, row in d.iterrows():
                    for map_func in map_funcs:
                        d = EtlTask.apply_map_func(
                            d, idx, row, map_func, map_name, map_type
                        )
            # TODO: dedup here
            output = output.append(d)
        output = output.reset_index()
        return output

    @staticmethod
    def import_map_funcs(
        config: Dict[str, Any]
    ) -> Dict[str, Dict[str, List[Callable]]]:
        """Import mapping functions according to source config.

        :rtype: Dict[str, Dict[str, List[Callable]]]
        :param config: the data source config
        :return: a dictionary containing all mapping functions
        """
        maps = {}
        if "mappings" in config:
            for m in config["mappings"]:
                maps[m] = {}
                # import mapping module from config
                mod = importlib.import_module("%s.%s" % (mapping.__name__, m))
                if m in INJECTED_MAPPINGS:
                    mod.MAPPING = INJECTED_MAPPINGS[m]
                # iterate through map type classes (Feature, Vertical, App, ...)
                # {mod: {cls: subcls: func: map_func}}
                maptree = EtlTask.extract_map_funcs_recursive(mod)
                # {mod: {type: [map_func, ...]}}
                # type = cls_type, subcls_type, func_type
                mapflat = flatten_dict(maptree)
                for types, map_func in mapflat.items():
                    ts = []
                    for name in types.split(","):
                        if is_camel(name):
                            # non-leaf node
                            ts += [decamelize(name)]
                        else:
                            # leaf node
                            ts += [name.split("_")[-1]]
                    for t in ts:
                        if t not in maps[m]:
                            maps[m][t] = []
                        maps[m][t] += [map_func]
        return maps

    @staticmethod
    def extract_map_funcs_recursive(mod: Callable, clsobj: Callable = None) -> Dict:
        """Extract map functions based on mapping config hierarchy.

        :param mod: the package of mapping configs
        :param clsobj: the module of the mapping config
        :return: extracted map functions in nested dictionary
        """
        maps = {}
        for cls, cobj in inspect.getmembers(
            mod if not clsobj else clsobj, predicate=inspect.isclass
        ):
            # filter only classes defined in the mapping module,
            # exclude imported or other builtin classes
            if hasattr(cobj, "__module__") and cobj.__module__ == mod.__name__:
                # iterate through static functions in the class
                for method, mobj in inspect.getmembers(
                    cobj, predicate=inspect.isroutine
                ):
                    # verify the method is defined directly on the class,
                    # not inherited or builtin methods,
                    # also verify it's a static method.
                    if method in cobj.__dict__ and isinstance(
                        cobj.__dict__[method], staticmethod
                    ):
                        if cls not in maps:
                            maps[cls] = {}
                        maps[cls][method] = mobj
                submaps = EtlTask.extract_map_funcs_recursive(mod, cobj)
                # merge dictionaries
                maps[cls] = {
                    **(maps[cls] if cls in maps else {}),
                    **(submaps if submaps else {}),
                }
        return maps

    @staticmethod
    def apply_map_func(
        df: DataFrame,
        idx: int,
        row: Series,
        map_func: Callable,
        map_name: str,
        map_type: str,
    ) -> DataFrame:
        """Apply map functions to a row of DataFrame.

        :param df: the DataFrame to apply
        :param idx: the row to apply
        :param row: the actual row (Series)
        :param map_func: the map function
        :param map_name: the name of the mapping
        :param map_type: the type of the mapping
        """
        with pd.option_context("mode.chained_assignment", None):
            map_result = map_func(row)
            if map_result:
                type_col = map_name + "_type"
                name_col = map_name + "_name"
                if type_col not in df.loc[idx].index:
                    df[type_col] = Series(dtype=object)
                if name_col not in df.loc[idx].index:
                    df[name_col] = Series(dtype=object)
                if "_" in map_type:
                    # non-leaft node, ignore returns
                    type_segments = map_type.split("_")
                    t = type_segments[-1]
                    n = "_".join(type_segments[0:-1])
                    if idx in df[type_col] and pd.notnull(df[type_col][idx]):
                        assert df[type_col][idx] == t, "%s != %s" % (
                            df[type_col][idx],
                            t,
                        )
                    if idx in df[name_col] and pd.notnull(df[name_col][idx]):
                        assert df[name_col][idx] == n, "%s != %s" % (
                            df[name_col][idx],
                            n,
                        )
                    df[type_col][idx] = t
                    df[name_col][idx] = n
                else:
                    # leaf node, use return value as name
                    if isinstance(map_result, str):
                        # Check duplicated mapping
                        assert (
                            pd.isnull(df[type_col][idx])
                            or df[type_col][idx] == map_type
                        )
                        assert (
                            pd.isnull(df[name_col][idx])
                            or df[name_col][idx] == map_result
                        )
                        df[type_col][idx] = map_type
                        df[name_col][idx] = map_result
                    elif isinstance(map_result, list):
                        df[type_col][idx] = map_type
                        # merge list if duplicated
                        if isinstance(df[name_col][idx], list):
                            df[name_col][idx] += map_result
                        elif pd.isnull(df[name_col][idx]):
                            df[name_col][idx] = map_result
                        else:
                            assert False, "Invalid data type found %s: %s" % (
                                map_type,
                                str(type(df[name_col][idx])),
                            )
                    else:
                        assert False, "Invalid mapping result %s: %s" % (
                            map_type,
                            str(map_result),
                        )
        return df

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
                    self.transformed[source] = transform_method(source, config)
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
                    with open(fpath, "w") as f:
                        f.write(r)
                log.info(
                    "%s-%s-%s/%s x %d pages loaded to file system."
                    % (stage, self.task, source, self.current_date.date(), len(raw))
                )
            elif isinstance(raw, dict):
                for i, r in raw.items():
                    fpath = self.get_or_create_filepath(source, config, stage, "fs", i)
                    with open(fpath, "w") as f:
                        f.write(r)
                log.info(
                    "%s-%s-%s/%s x %d pages loaded to file system."
                    % (stage, self.task, source, self.current_date.date(), len(raw))
                )
            else:
                with open(fpath, "w") as f:
                    f.write(raw)
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
                            ddf[rs[0]] = ddf[rs[0]].dt.strftime(DEFAULT_DATETIME_FORMAT)
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
        with open(fpath, "w") as f:
            output = self.convert_format(df)
            f.write(output)

    def convert_format(self, df: DataFrame, date_fields: List = None) -> str:
        """Convert DataFrame into destination format.

        The logic is based on task config (see `configs/*.py`).

        :param df: The DataFrame to be converted to destination format.
        :param date_fields: the date fields to convert to date string
        :return:
        """
        output = ""
        if date_fields:
            for date_field in date_fields:
                df[date_field] = df[date_field].dt.strftime(DEFAULT_DATETIME_FORMAT)
        if self.destinations["fs"]["file_format"] == "jsonl":
            output = ""
            # build json lines
            for row in df.iterrows():
                output += row[1].to_json() + "\n"
        elif self.destinations["fs"]["file_format"] == "json":
            output = "["
            # build json
            for row in df.iterrows():
                output += row[1].to_json() + ",\n"
            if len(output) > 2:
                output = output[0:-2] + "\n]"
        elif self.destinations["fs"]["file_format"] == "csv":
            output = df.to_csv(index=False)
        return output

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
                    self.get_filepath(
                        source, config, stage, "gcs", EtlTask.get_file_ext(fpath)
                    )
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
