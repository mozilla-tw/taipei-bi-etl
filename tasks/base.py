import errno
import glob
import re
import time
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
import pandas.io.json as pd_json
from typing import List, Optional

DEFAULT_DATE_FORMAT = '%Y-%m-%d'


def get_arg_parser() -> ArgumentParser:
    """ Parse arguments passed in EtlTask,
    --help will list all argument descriptions

    :rtype: ArgumentParser
    :return: properly configured argument parser to accept arguments
    """
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--task",
        default=None,
        help="The ETL task to run.",
    )
    parser.add_argument(
        "--source",
        default=None,
        help="The ETL data source to extract, use the name specified in settings.",
    )
    parser.add_argument(
        "--dest",
        default=None,
        help="The place to load transformed data to, can be 'fs' or 'gcs'.\n"
             "Default is 'gcs', "
             "which the intermediate output will still write to 'fs'.",
    )
    parser.add_argument(
        "--step",
        default=None,
        help="The ETL step to run to, "
             "can be 'extract', 'transform', 'load', or just the first letter. \n"
             "Default is 'load', which means go through the whole ETL process.",
    )
    parser.add_argument(
        "--date",
        type=lambda x: datetime.datetime.strptime(x, DEFAULT_DATE_FORMAT),
        default=datetime.datetime.today(),
        help="The base (latest) date of the data in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--period",
        type=int,
        default=30,
        help="Period of data in days.",
    )
    parser.add_argument(
        "--rm",
        default=False,
        action="store_true",
        help="Clean up cached files.",
    )
    return parser


class EtlTask:

    def __init__(self, args, sources, destinations, stage, task):
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
                files = glob.glob('{prefix}*-{task}-{source}/*'.format(
                    prefix=destinations['fs']['prefix'],
                    task=args.task, source=source))
                print(files)
                for f in files:
                    os.remove(f)
                print("Clean up cached files")
        self.task = task
        self.stage = stage
        self.args = args
        self.period = args.period
        self.current_date = args.date
        self.last_month = self.lookback_dates(args.date, args.period)
        self.sources = sources
        self.destinations = destinations
        self.raw = dict()
        self.extracted_base = dict()
        self.extracted = dict()
        self.transformed = dict()
        self.gcs = storage.Client()

    @staticmethod
    def lookback_dates(date, period):
        """Subtract date by period
        """
        return date - datetime.timedelta(days=period)

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
        :return: the converted DataFrame
        """
        ftype = 'json' if 'file_format' not in config else config['file_format']
        if ftype == 'json':
            extracted_json = EtlTask.json_extract(
                raw,
                None if 'json_path' not in config else config['json_path'])
            data = pd_json.loads(extracted_json)
            # TODO: make sure this is BQ-friendly before transform
            return pd_json.json_normalize(data)
        elif ftype == 'csv':
            return pd.read_csv(raw)

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
            will use self.current_date if not specified
        :return: a list of data file paths
        """
        return glob.glob('{prefix}{stage}-{task}-{source}/{filename}'.format(
            stage=stage, task=self.task, source=source,
            prefix=self.destinations[dest]['prefix'],
            filename=self.get_filename(source, config, stage, '*', date)))

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
            will use self.current_date if not specified
        :return: the data file path
        """
        return '{prefix}{stage}-{task}-{source}/{filename}'.format(
            stage=stage, task=self.task, source=source,
            prefix=self.destinations[dest]['prefix'],
            filename=self.get_filename(source, config, stage, page, date))

    def get_filename(self, source, config, stage, page=None, date=None) -> str:
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
            will use self.current_date if not specified
        :return: the data file name
        """
        date = self.current_date if date is None else date
        ftype = 'json' if 'file_format' not in config else config['file_format']
        if stage == 'raw':
            return '{date}.{page}.{ext}'.format(
                date=date.strftime(DEFAULT_DATE_FORMAT),
                ext=ftype,
                page=1 if page is None else page)
        else:
            if ftype == 'json':
                ftype = 'jsonl'     # enforce jsonl for BigQuery
            return '{date}.{ext}'.format(
                date=self.current_date.strftime(DEFAULT_DATE_FORMAT), ext=ftype)

    def get_or_create_filepath(self, source, config, stage, dest, page=None) -> str:
        """Get data file path,
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
        :return: the data file path
        """
        filename = self.get_filepath(source, config, stage, dest, page)
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
            will use self.current_date if not specified
        :return: the extracted DataFrame
        """
        # extract paged raw files
        if stage == 'raw':
            fpaths = self.get_filepaths(source, config, stage, 'fs', date)
        else:
            fpaths = [self.get_filepath(source, config, stage, 'fs', date)]
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
        print('%s-%s-%s/%s x %d pages extracted from file system'
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
            will use self.current_date if not specified
        :return: the extracted DataFrame
        """

        prefix = self.get_filepath(source, config, stage, 'gcs', '*', date)
        ext_regex = '([*0-9]+)\\.[A-z0-9]+$'
        ext_search = re.search(ext_regex, prefix)
        prefix = prefix[:ext_search.start()]
        blobs = self.gcs.list_blobs(self.destinations['gcs']['bucket'], prefix=prefix)

        i = 0
        for i, blob in enumerate(blobs):
            page = re.search(ext_regex, blob.name).group(1)
            blob.download_to_filename(
                self.get_filepath(source, config, stage, 'fs', page, date))

        print('%s-%s-%s/%s x %d pages extracted from google cloud storage'
              % (stage, self.task, source,
                 (self.current_date if date is None else date).date(), i + 1))
        return self.extract_via_fs(source, config, stage, date)

    def extract_via_api(self, source, config) -> DataFrame:
        """Extract data from API and convert into DataFrame
        based on task config, see `configs/*.py`

        :rtype: DataFrame
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :return: the extracted DataFrame
        """
        # API paging
        if 'page_size' in config:
            limit = config['page_size']
            url = config['url'].format(api_key=config['api_key'],
                                       start_date=self.last_month.strftime(
                                           config['date_format']),
                                       end_date=self.current_date.strftime(
                                           config['date_format']),
                                       page=1, limit=limit)
            r = requests.get(url, allow_redirects=True)
            raw = [r.text]
            extracted = self.convert_df(raw[0], config)
            count = int(self.json_extract(raw[0], config['json_path_page_count']))
            if count is None or int(count) <= 1:
                self.raw[source] = raw
                print('%s-%s-%s/%s x 1 page extracted from API'
                      % ('raw', self.task, source,
                         self.current_date.date()))
                return extracted
            request_interval = \
                config['request_interval'] if 'request_interval' in config else 1
            for page in range(2, count):
                print('waiting for %s page %d' % (source, page))
                time.sleep(request_interval)
                url = config['url'].format(api_key=config['api_key'],
                                           start_date=self.last_month.strftime(
                                               config['date_format']),
                                           end_date=self.current_date.strftime(
                                               config['date_format']),
                                           page=page, limit=limit)
                r = requests.get(url, allow_redirects=True)
                raw += [r.text]
                extracted = extracted.append(self.convert_df(raw[page - 1], config))
            extracted = extracted.reset_index(drop=True)
            self.raw[source] = raw
            print('%s-%s-%s/%s x %d pages extracted from API'
                  % ('raw', self.task, source,
                     self.current_date.date(), count))
            return extracted
        else:
            url = config['url'].format(api_key=config['api_key'],
                                       start_date=self.last_month.strftime(
                                           config['date_format']),
                                       end_date=self.current_date.strftime(
                                           config['date_format']))
            r = requests.get(url, allow_redirects=True)
            raw = r.text
            self.raw[source] = raw
            print('%s-%s-%s/%s extracted from API'
                  % ('raw', self.task, source,
                     self.current_date.date()))
            return self.convert_df(raw, config)

    def extract_via_bq(self, source, config) -> DataFrame:
        """Extract data from Google BigQuery and convert into DataFrame
        based on task config, see `configs/*.py`

        :rtype: DataFrame
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :return: the extracted DataFrame
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
                    from_date=self.last_month.strftime(config['date_format']),
                    to_date=self.current_date.strftime(config['date_format']))
        df = pdbq.read_gbq(query)
        print('%s-%s-%s/%s w/t %d records extracted from BigQuery'
              % ('raw', self.task, source,
                 self.current_date.date(), len(df.index)))
        return df

    def extract(self):
        """Iterate through data source settings in task config (see `configs/*.py`)
        and extract them accordingly based on source type and the source argument.
        see also `get_arg_parser()`.

        """
        for source in self.sources:
            if not self.args.source or self.args.source == source:
                if self.sources[source]['type'] == 'api':
                    config = self.sources[source]
                    # use file cache to prevent calling partner API too many times
                    if 'cache_file' in config and config['cache_file']:
                        if not self.is_cached(source, config):
                            self.extracted[source] = self.extract_via_api(
                                source, config)
                            self.load_to_fs(source, config)
                            if self.args.dest != 'fs':
                                self.load_to_gcs(source, config)
                        else:
                            self.extracted[source] = self.extract_via_fs(
                                source, config)
                    else:
                        self.extracted[source] = self.extract_via_api(source, config)
                        if self.args.dest != 'fs' \
                                and 'force_load' in config and config['force_load']:
                            self.load_to_gcs(source, config)
                    # Extract data from previous date for validation
                    yesterday = EtlTask.lookback_dates(self.current_date, 1)
                    if self.args.dest != 'fs':
                        self.extracted_base[source] = self.extract_via_gcs(
                            source, config, 'raw', yesterday)
                    else:
                        self.extracted_base[source] = self.extract_via_fs(
                            source, config, 'raw', yesterday)
                elif self.sources[source]['type'] == 'bq':
                    self.extracted[source] = self.extract_via_bq(
                        source, self.sources[source])

    def transform(self):
        """Iterate through data source settings in task config (see `configs/*.py`)
        and transform extracted DataFrames accordingly
        based on the source argument (see also `get_arg_parser()`),
        will need to create a function for each data source when inheriting this class.
        e.g. def transform_google_search(source, config) for google_search data source.
        source: name of the data source to be extracted,
        specified in task config, see `configs/*.py`
        config: config of the data source to be extracted,
        specified in task config, see `configs/*.py`

        """
        for source in self.sources:
            if not self.args.source or self.args.source == source:
                config = self.sources[source]
                assert self.extracted is not None
                transform_method = getattr(self, 'transform_{}'.format(source))
                self.transformed[source] = transform_method(source, config)
                print('%s-%s-%s/%s w/t %d records transformed'
                      % (self.stage, self.task, source,
                         self.current_date.date(), len(self.transformed[source].index)))

    def load_to_fs(self, source, config, stage='raw'):
        """Load data into file system based on destination settings
        in task config (see `configs/*.py`).

        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param stage: the stage of the loaded data, could be raw/staging/production.
        """
        fpath = self.get_or_create_filepath(source, config, stage, 'fs')
        if stage == 'raw':
            # write multiple raw files if paged
            raw = self.raw[source]
            if isinstance(raw, list):
                for i, r in enumerate(raw):
                    fpath = self.get_or_create_filepath(
                        source, config, stage, 'fs', i + 1)
                    with open(fpath, 'w') as f:
                        f.write(r)
                print('%s-%s-%s/%s x %d pages loaded to file system.' %
                      (stage, self.task, source, self.current_date.date(), len(raw)))
            else:
                with open(fpath, 'w') as f:
                    f.write(raw)
                    print('%s-%s-%s/%s x 1 page loaded to file system.' %
                          (stage, self.task, source, self.current_date.date()))
        else:
            with open(fpath, 'w') as f:
                output = ''
                if self.destinations['fs']['file_format'] == 'json':
                    output = ''
                    # build json lines
                    for row in self.transformed[source].iterrows():
                        output += row[1].to_json() + '\n'
                elif self.destinations['fs']['file_format'] == 'csv':
                    output = self.transformed[source].to_csv()
                f.write(output)
                print('%s-%s-%s/%s loaded to file system.' %
                      (stage, self.task, source, self.current_date.date()))

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
        blob = bucket.blob(self.get_filepath(source, config, stage, 'gcs'))
        blob.upload_from_filename(self.get_filepath(source, config, stage, 'fs'))
        print('%s-%s-%s/%s loaded to GCS.' %
              (stage, self.task, source, self.current_date.date()))

    def load(self):
        """Iterate through data source settings in task config (see `configs/*.py`)
        and load transformed data accordingly based on the destination argument,
        see also `get_arg_parser()`.

        """
        for source in self.sources:
            if not self.args.source or self.args.source == source:
                config = self.sources[source]
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
