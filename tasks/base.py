import errno
import glob
import re
import time
from collections import Counter
from io import StringIO
from argparse import ArgumentParser
import os
import os.path
import requests
import datetime
import pandas as pd
from pandas import DataFrame
import pandas_gbq as pdbq
from google.cloud import storage
import json
import numpy as np
import pandas.io.json as pd_json
from typing import List, Optional, Tuple, Union, Dict
from pandas_schema import Column, Schema
from pandas_schema.validation import IsDtypeValidation
import pytz
import logging

log = logging.getLogger(__name__)

DEFAULT_DATE_FORMAT = '%Y-%m-%d'
DEFAULT_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_TZ_FORMAT = '%+03d:00'
EXT_REGEX = '([*0-9A-z]+)\\.[A-z0-9]+$'
DEFAULT_PATH_FORMAT = '{prefix}{stage}-{task}-{source}'


def get_arg_parser(**kwargs) -> ArgumentParser:
    """ Parse arguments passed in EtlTask,
    --help will list all argument descriptions

    :rtype: ArgumentParser
    :return: properly configured argument parser to accept arguments
    """
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--debug",
        default=False if 'debug' not in kwargs else kwargs['debug'],
        action="store_true",
        help="Run tasks in DEBUG mode, will use debugging configs in /configs/debug/*",
    )
    parser.add_argument(
        "--task",
        default=None if 'task' not in kwargs else kwargs['task'],
        help="The ETL task to run.",
    )
    parser.add_argument(
        "--source",
        default=None if 'source' not in kwargs else kwargs['source'],
        help="The ETL data source to extract, use the name specified in settings.",
    )
    parser.add_argument(
        "--dest",
        default=None if 'dest' not in kwargs else kwargs['dest'],
        help="The place to load transformed data to, can be 'fs' or 'gcs'.\n"
             "Default is 'gcs', "
             "which the intermediate output will still write to 'fs'.",
    )
    parser.add_argument(
        "--step",
        default=None if 'step' not in kwargs else kwargs['step'],
        help="The ETL step to run to, "
             "can be 'extract', 'transform', 'load', or just the first letter. \n"
             "Default is 'load', which means go through the whole ETL process.",
    )
    parser.add_argument(
        "--date",
        type=lambda x: datetime.datetime.strptime(x, DEFAULT_DATE_FORMAT),
        default=datetime.datetime.today() if 'date' not in kwargs else kwargs['date'],
        help="The base (latest) date of the data in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--period",
        type=int,
        default=30 if 'period' not in kwargs else kwargs['period'],
        help="Period of data in days.",
    )
    parser.add_argument(
        "--rm",
        default=False if 'rm' not in kwargs else kwargs['rm'],
        action="store_true",
        help="Clean up cached files.",
    )
    return parser


class EtlTask:

    def __init__(self, args, sources, schema, destinations, stage, task):
        """Initiate parameters and client libraries for ETL task.

        :param args: args passed from command line,
        see `get_arg_parser()`
        :param sources: data source to be extracted,
        specified in task config, see `configs/*.py`
        :param destinations: destinations to load data to,
        specified in task config, see `configs/*.py`
        :param stage: the stage of the loaded data, could be staging/production.
        """
        # Clear cached files
        if args.rm:
            for source in sources:
                files = []
                files += glob.glob(EtlTask.get_path_format(True).format(
                    prefix=destinations['fs']['prefix'],
                    stage='raw', task=args.task, source=source))
                files += glob.glob(EtlTask.get_path_format(True).format(
                    prefix=destinations['fs']['prefix'],
                    stage=stage, task=args.task, source=source))
                for f in files:
                    log.info("Removing cached file: %s" % f)
                    os.remove(f)
        self.task = task
        self.stage = stage
        self.args = args
        self.period = args.period
        self.current_date = args.date
        self.last_month = self.lookback_dates(args.date, args.period)
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
    def get_country_tz(country_code) -> pytz.UTC:
        """Get the default timezone for specified country code,
        if covered multiple timezone, pick the most common one.

        :rtype: pytz.UTC
        :return: the default (major) timezone of the country
        :param country_code: the 2 digit country code to get timezone
        """
        if country_code not in pytz.country_timezones:
            # FIXME: workaround here for pytz doesn't support XK for now.
            tzmap = {'XK': 'CET'}
            if country_code in tzmap:
                return pytz.timezone(tzmap[country_code])
            log.warn('timezone not found for %s, return UTC' % country_code)
            return pytz.utc
        timezones = pytz.country_timezones[country_code]
        offsets = []
        for timezone in timezones:
            offsets += [pytz.timezone(timezone).utcoffset(
                datetime.datetime.now()).seconds / 3600]
        offset_count = Counter(offsets)
        max_count = -1
        max_offset = None
        for k, v in offset_count.items():
            if v > max_count:
                max_count = v
                max_offset = k
        return pytz.timezone(timezones[offsets.index(max_offset)])

    @staticmethod
    def get_country_tz_str(country_code) -> str:
        """Get the default timezone string (e.g. +08:00) for specified country code,
        if covered multiple timezone, pick the most common one.

        :rtype: str
        :param country_code: the 2 digit country code to get timezone
        :return: the default (major) timezone string of the country in +08:00 format.
        """
        return EtlTask.get_tz_str(EtlTask.get_country_tz(country_code))

    @staticmethod
    def get_tz_str(timezone) -> str:
        """Convert timezone to offset string (e.g. +08:00)

        :rtype: str
        :param timezone: pytz.UTC
        :return: the timezone offset string in +08:00 format.
        """
        return DEFAULT_TZ_FORMAT % (timezone.utcoffset(
            datetime.datetime.now()).seconds / 3600)

    @staticmethod
    def lookback_dates(date, period):
        """Subtract date by period
        """
        return date - datetime.timedelta(days=period)

    @staticmethod
    def lookfoward_dates(date, period):
        """Subtract date by period
        """
        return date + datetime.timedelta(days=period)

    @staticmethod
    def json_extract(json_str, path) -> Optional[str]:
        """Extract nested json element by path,
        currently dont' support nested json array in path

        :rtype: str
        :param json_str: original json in string format
        :param path: path of the element in string format, e.g. 'response.data'
        :return: the extracted json element in string format
        """
        j = json.loads(json_str)
        if path:
            for i in path.split("."):
                if i in j:
                    j = j[i]
                else:
                    return None
        return json.dumps(j)

    @staticmethod
    def convert_df(raw, config) -> DataFrame:
        """Convert raw string to DataFrame, currently only supports json/csv.

        :rtype: DataFrame
        :param raw: the raw source string in json/csv format,
            this is to be converted to DataFrame
        :param config: the config of the data source specified in task config,
            see `configs/*.py`
        :return: the converted `DataFrame`
        """
        ftype = 'json' if 'file_format' not in config else config['file_format']
        df = None
        if ftype == 'json':
            extracted_json = EtlTask.json_extract(
                raw,
                None if 'json_path' not in config else config['json_path'])
            data = pd_json.loads(extracted_json)
            df = pd_json.json_normalize(data)
        elif ftype == 'csv':
            if 'header' in config:
                df = pd.read_csv(StringIO(raw), names=config['header'])
            else:
                df = pd.read_csv(StringIO(raw))
        # convert timezone according to config
        tz = None
        if 'timezone' in config:
            tz = pytz.timezone(config['timezone'])
        elif 'country_code' in config:
            tz = EtlTask.get_country_tz(config['country_code'])
        # TODO: support multiple countries/timezones in the future if needed
        if tz is not None and 'date_fields' in config:
            df['tz'] = EtlTask.get_tz_str(tz)
            for date_field in config['date_fields']:
                df[date_field].dt.tz_localize(tz).dt.tz_convert(pytz.utc)
                df[date_field] = df[date_field].astype('datetime64[ns]')
        return df

    @staticmethod
    def get_page_ext(fpath):
        return re.search(EXT_REGEX, fpath).group(1)

    @staticmethod
    def get_prefix(prefix):
        ext_search = re.search(EXT_REGEX, prefix)
        return prefix[:ext_search.start()]

    @staticmethod
    def get_path_format(wildcard=False):
        if wildcard:
            return DEFAULT_PATH_FORMAT + '/*'
        else:
            return DEFAULT_PATH_FORMAT + '/{filename}'

    def get_filepaths(self, source, config, stage, dest, date=None) -> List[str]:
        """Get existing data file paths with wildcard page number

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
        if config['type'] == 'gcs':
            if dest == 'gcs':
                prefix = config['prefix']
            else:
                prefix = self.destinations[dest]['prefix']
            return glob.glob(prefix + config['path'] + config['filename'])
        else:
            return glob.glob(EtlTask.get_path_format().format(
                stage=stage, task=self.task, source=source,
                prefix=self.destinations[dest]['prefix'],
                filename=self.get_filename(source, config, stage, dest, '*', date)))

    def get_filepath(self, source, config, stage, dest, page=None, date=None) -> str:
        """Get data file path,
        which the format would be {prefix}{stage}-{task}-{source}/{filename}

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
        if config['type'] == 'gcs':
            if dest == 'gcs':
                prefix = config['prefix']
            else:
                prefix = self.destinations[dest]['prefix']
            fpath = prefix + config['path'] + config['filename']
            if page is not None:
                fpath = fpath.replace('*', page)
            return fpath
        else:
            return EtlTask.get_path_format().format(
                stage=stage, task=self.task, source=source,
                prefix=self.destinations[dest]['prefix'],
                filename=self.get_filename(source, config, stage, dest, page, date))

    def get_filename(self, source, config, stage, dest, page=None, date=None) -> str:
        """Get data file name,
        which the format would be {date}.{page}.{ext} for raw data,
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
        if stage == 'raw':
            ftype = 'json' if 'file_format' not in config else config['file_format']
            return '{date}.{page}.{ext}'.format(
                date=date.strftime(DEFAULT_DATE_FORMAT),
                ext=ftype,
                page=1 if page is None else page)
        else:
            dest_config = self.destinations['fs']
            ftype = 'jsonl' if 'file_format' not in dest_config \
                else dest_config['file_format']
            return '{date}.{ext}'.format(
                date=date.strftime(DEFAULT_DATE_FORMAT), ext=ftype)

    def get_or_create_filepath(self, source, config, stage, dest,
                               page=None, date=None) -> str:
        """Get data file path for loading,
        which the format would be {prefix}{stage}-{task}-{source}/{filename}.
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

    def is_cached(self, source, config) -> bool:
        """Check whether a raw data is cached,
        currently only used for raw data extracted from API.

        :rtype: bool
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :return: whether a data file is cached in local file system
        """
        fpath = self.get_filepath(source, config, 'raw', 'fs')
        return os.path.isfile(fpath)

    def get_target_dataframe(self, schema=None) -> DataFrame:
        """Get an empty DataFrame with target schema

        :param schema: list of tuples(column name, numpy data type),
            see `configs/*.py`
        :rtype: DataFrame
        :return: an empty DataFrame with target schema
        """
        return DataFrame(
            np.empty(0, dtype=np.dtype(self.raw_schema if schema is None else schema)))

    def extract_via_fs(self, source, config, stage='raw', date=None) -> DataFrame:
        """Extract data from file system and convert into DataFrame
        based on task config, see `configs/*.py`

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
        if stage == 'raw':
            fpaths = self.get_filepaths(source, config, stage, 'fs', date)
        else:
            fpaths = [self.get_filepath(source, config, stage, 'fs', date)]
        if 'iterator' in config:
            extracted = None if 'iterator' not in config else dict()
            for fpath in fpaths:
                with open(fpath, 'r') as f:
                    raw = f.read()
                    it = EtlTask.get_page_ext(fpath)
                    self.raw[it] = raw
                    extracted[it] = self.convert_df(raw, config)
            log.info('%s-%s-%s/%s x %d iterators extracted from file system'
                  % (stage, self.task, source,
                     (self.current_date if date is None else date).date(),
                     len(fpaths)))
        else:
            extracted = None
            for fpath in fpaths:
                with open(fpath, 'r') as f:
                    raw = f.read()
                    if extracted is None:
                        self.raw[source] = [raw]
                        extracted = self.convert_df(raw, config)
                    else:
                        self.raw[source] += [raw]
                        extracted = extracted.append(self.convert_df(raw, config))
            extracted = extracted.reset_index(drop=True)
            log.info('%s-%s-%s/%s x %d pages extracted from file system'
                  % (stage, self.task, source,
                     (self.current_date if date is None else date).date(), len(fpaths)))
        return extracted

    def extract_via_gcs(self, source, config, stage='raw', date=None) -> DataFrame:
        """Downloads blobs from Google Cloud Storage bucket,
        extract them from file system and convert into DataFrame
        based on task config, see `configs/*.py`

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
        if config['type'] == 'gcs':
            bucket = config['bucket']
            prefix = self.get_filepath(source, config, stage, 'gcs')
        else:
            bucket = self.destinations['gcs']['bucket']
            prefix = self.get_filepath(source, config, stage, 'gcs', '*', date)
            prefix = EtlTask.get_prefix(prefix)
        blobs = self.gcs.list_blobs(bucket, prefix=prefix)

        i = 0
        is_empty = True
        for i, blob in enumerate(blobs):
            is_empty = False
            page = EtlTask.get_page_ext(blob.name)
            filepath = self.get_filepath(source, config, stage, 'fs', page, date)
            blob.download_to_filename(filepath)

        if not is_empty:
            if config['type'] == 'gcs':
                log.info('%s x %d pages extracted from google cloud storage'
                      % (prefix, i + 1))
            else:
                log.info('%s-%s-%s/%s x %d pages extracted from google cloud storage'
                      % (stage, self.task, source,
                         (self.current_date if date is None else date).date(), i + 1))
            return self.extract_via_fs(source, config, stage, date)
        else:
            return DataFrame()

    def extract_via_api(self, source, config, stage='raw', date=None) \
            -> Union[DataFrame, Dict[str, DataFrame]]:
        """Extract data from API and convert into DataFrame
        based on task config, see `configs/*.py`

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
        start_date = self.last_month.strftime(config['date_format']) if date is None \
            else EtlTask.lookback_dates(date, self.period)
        end_date = self.current_date.strftime(config['date_format']) if date is None \
            else date
        request_interval = \
            config['request_interval'] if 'request_interval' in config else 1
        if 'iterator' in config:
            raw = dict()
            extracted = dict()
            for it in config['iterator']:
                log.debug('waiting for %s iterator %d' % (source, it))
                time.sleep(request_interval)
                it = str(it)
                url = config['url'].format(api_key=config['api_key'],
                                           start_date=start_date,
                                           end_date=end_date,
                                           iterator=it)
                r = requests.get(url, allow_redirects=True)
                raw[it] = r.text
                extracted[it] = self.convert_df(raw[it], config)
            self.raw[source] = raw
            log.info('%s-%s-%s/%s x %d iterators extracted from API'
                  % (stage, self.task, source,
                     self.current_date.date(), len(extracted)))
            return extracted
        elif 'page_size' in config:
            limit = config['page_size']
            url = config['url'].format(api_key=config['api_key'],
                                       start_date=start_date,
                                       end_date=end_date,
                                       page=1, limit=limit)
            r = requests.get(url, allow_redirects=True)
            raw = [r.text]
            extracted = self.convert_df(raw[0], config)
            count = int(self.json_extract(raw[0], config['json_path_page_count']))
            if count is None or int(count) <= 1:
                self.raw[source] = raw
                log.info('%s-%s-%s/%s x 1 page extracted from API'
                      % (stage, self.task, source,
                         self.current_date.date()))
                return extracted
            for page in range(2, count):
                log.debug('waiting for %s page %d' % (source, page))
                time.sleep(request_interval)
                url = config['url'].format(api_key=config['api_key'],
                                           start_date=start_date,
                                           end_date=end_date,
                                           page=page, limit=limit)
                r = requests.get(url, allow_redirects=True)
                raw += [r.text]
                extracted = extracted.append(self.convert_df(raw[page - 1], config))
            extracted = extracted.reset_index(drop=True)
            self.raw[source] = raw
            log.info('%s-%s-%s/%s x %d pages extracted from API'
                  % (stage, self.task, source,
                     self.current_date.date(), count))
            return extracted
        else:
            url = config['url'].format(api_key=config['api_key'],
                                       start_date=start_date,
                                       end_date=end_date)
            r = requests.get(url, allow_redirects=True)
            raw = r.text
            self.raw[source] = raw
            log.info('%s-%s-%s/%s extracted from API'
                  % ('raw', self.task, source,
                     self.current_date.date()))
            return self.convert_df(raw, config)

    def extract_via_api_or_cache(self, source, config, stage='raw', date=None) \
            -> Tuple[DataFrame, DataFrame]:
        """Extract data from API and convert into DataFrame
        based on task config, see `configs/*.py`

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
        if 'cache_file' in config and config['cache_file']:
            if not self.is_cached(source, config):
                extracted = self.extract_via_api(
                    source, config, stage, date)
                self.load_to_fs(source, config)
                if self.args.dest != 'fs':
                    self.load_to_gcs(source, config)
            else:
                extracted = self.extract_via_fs(
                    source, config)
        else:
            extracted = self.extract_via_api(source, config, stage, date)
            if self.args.dest != 'fs' \
                    and 'force_load_cache' in config and config['force_load_cache']:
                self.load_to_gcs(source, config)
        # Extract data from previous date for validation
        yesterday = EtlTask.lookback_dates(self.current_date, 1)
        if self.args.dest != 'fs':
            extracted_base = self.extract_via_gcs(
                source, config, 'raw', yesterday)
        else:
            extracted_base = self.extract_via_fs(
                source, config, 'raw', yesterday)
        return extracted, extracted_base

    def extract_via_bq(self, source, config) -> DataFrame:
        """Extract data from Google BigQuery and convert into DataFrame
        based on task config, see `configs/*.py`

        :rtype: DataFrame
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :return: the extracted `DataFrame`
        """
        query = ''
        if 'udf' in config:
            for udf in config['udf']:
                with open('udf/{}.sql'.format(udf)) as f:
                    query += f.read()
        if 'udf_js' in config:
            for udf_js in config['udf_js']:
                with open('udf_js/{}.sql'.format(udf_js)) as f:
                    query += f.read()
        if 'query' in config:
            with open('sql/{}.sql'.format(config['query'])) as f:
                query += f.read().format(
                    project=config['project'],
                    dataset=config['dataset'],
                    table=config['table'],
                    start_date=self.last_month.strftime(config['date_format']),
                    end_date=self.current_date.strftime(config['date_format']))
        df = pdbq.read_gbq(query)
        log.info('%s-%s-%s/%s w/t %d records extracted from BigQuery'
              % ('raw', self.task, source,
                 self.current_date.date(), len(df.index)))
        return df

    @staticmethod
    def extract_via_const(source, config) -> DataFrame:
        """Extract data from Google BigQuery and convert into DataFrame
        based on task config, see `configs/*.py`

        :rtype: DataFrame
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :return: the extracted `DataFrame`
        """
        return DataFrame(config['values'])

    def extract(self):
        """Iterate through data source settings in task config (see `configs/*.py`)
        and extract them accordingly based on source type and the source argument.
        see also `get_arg_parser()`.

        """
        for source in self.sources:
            if not self.args.source or source in self.args.source.split(','):
                config = self.sources[source]
                if self.sources[source]['type'] == 'api':
                    self.extracted[source], self.extracted_base[source] \
                        = self.extract_via_api_or_cache(source, config)
                elif self.sources[source]['type'] == 'gcs':
                    self.extracted[source] = self.extract_via_gcs(source, config)
                elif self.sources[source]['type'] == 'bq':
                    self.extracted[source] = self.extract_via_bq(source, config)
                elif self.sources[source]['type'] == 'const':
                    self.extracted[source] = self.extract_via_const(source, config)

    def transform(self):
        """Iterate through data source settings in task config (see `configs/*.py`)
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
            if not self.args.source or source in self.args.source.split(','):
                config = self.sources[source]
                # only transform data to be loaded
                if 'load' in config and config['load']:
                    assert self.extracted is not None
                    transform_method = getattr(self, 'transform_{}'.format(source))
                    self.transformed[source] = transform_method(source, config)
                    errors = self.schema.validate(self.transformed[source])
                    error_msg = ''
                    for error in errors:
                        error_msg += error.message + '\n'
                    assert len(errors) == 0, error_msg
                    log.info('%s-%s-%s/%s w/t %d records transformed'
                          % (self.stage, self.task, source,
                             self.current_date.date(),
                             len(self.transformed[source].index)))

    def load_to_fs(self, source, config, stage='raw'):
        """Load data into file system based on destination settings
        in task config (see `configs/*.py`).

        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param stage: the stage of the loaded data, could be raw/staging/production.
        """
        if stage == 'raw':
            fpath = self.get_or_create_filepath(source, config, stage, 'fs')
            # write multiple raw files if paged
            raw = self.raw[source]
            if isinstance(raw, list):
                for i, r in enumerate(raw):
                    fpath = self.get_or_create_filepath(
                        source, config, stage, 'fs', i + 1)
                    with open(fpath, 'w') as f:
                        f.write(r)
                log.info('%s-%s-%s/%s x %d pages loaded to file system.' %
                      (stage, self.task, source, self.current_date.date(), len(raw)))
            elif isinstance(raw, dict):
                for i, r in raw.items():
                    fpath = self.get_or_create_filepath(
                        source, config, stage, 'fs', i)
                    with open(fpath, 'w') as f:
                        f.write(r)
                log.info('%s-%s-%s/%s x %d pages loaded to file system.' %
                      (stage, self.task, source, self.current_date.date(), len(raw)))
            else:
                with open(fpath, 'w') as f:
                    f.write(raw)
                    log.info('%s-%s-%s/%s x 1 page loaded to file system.' %
                          (stage, self.task, source, self.current_date.date()))
        else:
            df = self.transformed[source]
            if 'date_field' in self.destinations['fs']:
                ds = df[self.destinations['fs']['date_field']].dt.date.unique()
                # load files by date
                for d in ds:
                    ddf = df[
                        df[self.destinations['fs']['date_field']].dt.date == d].copy()
                    # Fix date format for BigQuery (only support dash notation)
                    for rs in self.raw_schema:
                        if rs[1] == np.datetime64:
                            ddf[rs[0]] = ddf[rs[0]].dt.strftime(DEFAULT_DATETIME_FORMAT)
                    self.convert_file(ddf, config, source, stage, d)
                log.info('%s-%s-%s/%s x %d files loaded to file system.' %
                      (stage, self.task, source, self.current_date.date(), len(ds)))
            else:
                self.convert_file(df, config, source, stage)
                log.info('%s-%s-%s/%s x 1 files loaded to file system.' %
                      (stage, self.task, source, self.current_date.date()))

    def convert_file(self, df, config, source, stage, date=None):
        date = self.current_date if date is None else date
        fpath = self.get_or_create_filepath(
            source, config, stage, 'fs', None, date)
        with open(fpath, 'w') as f:
            output = ''
            if self.destinations['fs']['file_format'] == 'jsonl':
                output = ''
                # build json lines
                for row in df.iterrows():
                    output += row[1].to_json() + '\n'
            elif self.destinations['fs']['file_format'] == 'json':
                output = '['
                # build json
                for row in df.iterrows():
                    output += row[1].to_json() + ',\n'
                if len(output) > 2:
                    output = output[0:-2] + '\n]'
            elif self.destinations['fs']['file_format'] == 'csv':
                output = df.to_csv(index=False)
            f.write(output)

    def load_to_gcs(self, source, config, stage='raw'):
        """Load data into Google Cloud Storage based on destination settings
        in task config (see `configs/*.py`).

        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param stage: the stage of the loaded data, could be raw/staging/production.
        """
        bucket = self.gcs.bucket(self.destinations['gcs']['bucket'])
        fl = 0
        if stage == 'raw':
            fpaths = self.get_filepaths(source, config, stage, 'fs')
            fl = len(fpaths)
            for fpath in fpaths:
                blob = bucket.blob(self.get_filepath(source, config, stage, 'gcs',
                                                     EtlTask.get_page_ext(fpath)))
                blob.upload_from_filename(fpath)
        else:
            # load files by date
            df = self.transformed[source]

            if 'date_field' in self.destinations['fs']:
                ds = df[self.destinations['fs']['date_field']].dt.date.unique()
                fl = len(ds)
                for d in ds:
                    blob = bucket.blob(
                        self.get_filepath(source, config, stage, 'gcs', None, d))
                    blob.upload_from_filename(
                        self.get_filepath(source, config, stage, 'fs', None, d))
            else:
                fl = 1
                blob = bucket.blob(self.get_filepath(source, config, stage, 'gcs'))
                blob.upload_from_filename(self.get_filepath(source, config, stage, 'fs'))
        log.info('%s-%s-%s/%s x %d files loaded to GCS.' %
              (stage, self.task, source, self.current_date.date(), fl))

    def load(self):
        """Iterate through data source settings in task config (see `configs/*.py`)
        and load transformed data accordingly based on the destination argument,
        see also `get_arg_parser()`.

        """
        for source in self.sources:
            if not self.args.source or source in self.args.source.split(','):
                config = self.sources[source]
                if 'load' in config and config['load']:
                    assert self.transformed[source] is not None
                    self.load_to_fs(source, config, self.stage)
                    if self.args.dest != 'fs':
                        self.load_to_gcs(source, config, self.stage)

    def run(self):
        """Run the whole ETL process based on the step argument.
        see `get_arg_parser()`.

        """
        if self.args.step and self.args.step[0].upper() not in ['E', 'T', 'L']:
            raise ValueError('Invalid argument specified.')
        if not self.args.step or self.args.step[0].upper() in ['E', 'T', 'L']:
            self.extract()
        if not self.args.step or self.args.step[0].upper() in ['T', 'L']:
            self.transform()
        if not self.args.step or self.args.step[0].upper() in ['L']:
            self.load()
