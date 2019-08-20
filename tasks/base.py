import errno
import glob
import time
from argparse import ArgumentParser
import os
import os.path
import requests
import datetime
import pandas as pd
import pandas_gbq as pdbq
from google.cloud import storage
import json
import pandas.io.json as pd_json

DEFAULT_DATE_FORMAT = '%Y-%m-%d'


def get_arg_parser():
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
             "Default is 'gcs', which the intermediate output will still write to 'fs'",
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

    def __init__(self, args, sources, destinations, stage):
        if args.rm:
            for source in sources:
                files = glob.glob('{prefix}*-{task}-{source}/*'.format(
                    prefix=destinations['fs']['prefix'],
                    task=args.task, source=source))
                print(files)
                for f in files:
                    os.remove(f)
                print("Clean up cached files")
        self.stage = stage
        self.args = args
        self.period = args.period
        self.current_date = args.date
        self.last_month = args.date
        self.init_dates()
        self.sources = sources
        self.destinations = destinations
        self.raw = dict()
        self.extracted = dict()
        self.transformed = dict()
        self.gcs = storage.Client()

    def init_dates(self):
        self.last_month = self.current_date - datetime.timedelta(days=self.period)

    def get_filepaths(self, source, config, stage, dest):
        return glob.glob('{prefix}{stage}-{task}-{source}/{filename}'.format(
            stage=stage, task=self.args.task, source=source,
            prefix=self.destinations[dest]['prefix'],
            filename=self.get_filename(source, config, stage, '*')))

    def get_filepath(self, source, config, stage, dest, page=None):
        return '{prefix}{stage}-{task}-{source}/{filename}'.format(
            stage=stage, task=self.args.task, source=source,
            prefix=self.destinations[dest]['prefix'],
            filename=self.get_filename(source, config, stage, page))

    def get_filename(self, source, config, stage, page=None):
        ftype = 'json' if 'file_format' not in config else config['file_format']
        if stage == 'raw':
            return '{date}.{page}.{ext}'.format(
                date=self.current_date.strftime(DEFAULT_DATE_FORMAT),
                ext=ftype,
                page=1 if page is None else page)
        else:
            return '{date}.{ext}'.format(
                date=self.current_date.strftime(DEFAULT_DATE_FORMAT), ext=ftype)

    def get_or_create_filepath(self, source, config, stage, dest, page=None):
        filename = self.get_filepath(source, config, stage, dest, page)
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        return filename

    def is_cached(self, source, config):
        fpath = self.get_filepath(source, config, 'raw', 'fs')
        return os.path.isfile(fpath)

    def json_extract(self, json_str, path):
        j = json.loads(json_str)
        if path:
            for i in path.split("."):
                if i in j:
                    j = j[i]
                else:
                    return None
        return json.dumps(j)

    def convert_df(self, extracted, config):
        ftype = 'json' if 'file_format' not in config else config['file_format']
        if ftype == 'json':
            extracted_json = self.json_extract(
                    extracted,
                    None if 'json_path' not in config else config['json_path'])
            data = pd_json.loads(extracted_json)
            # TODO: make sure this is BQ-friendly before transform
            return pd_json.json_normalize(data)
        elif ftype == 'csv':
            return pd.read_csv(extracted)

    def extract_via_fs(self, source, config, stage='raw'):
        # extract paged raw files
        if stage == 'raw':
            fpaths = self.get_filepaths(source, config, stage, 'fs')
        else:
            fpaths = [self.get_filepath(source, config, stage, 'fs')]
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
        print('%s x %d pages extracted from file system' % (source, len(fpaths)))
        return extracted

    def extract_via_api(self, source, config):
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
                print('%s x 1 page extracted from API' % source)
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
                extracted = extracted.append(self.convert_df(raw[page-1], config))
            extracted = extracted.reset_index(drop=True)
            self.raw[source] = raw
            print('%s x %d pages extracted from API' % (source, count))
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
            print('%s extracted from API' % source)
            return self.convert_df(raw, config)

    def extract_via_bq(self, source, config):
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
                    from_date=self.last_month.strftime(config['date_format']))
        df = pdbq.read_gbq(query)
        print('%s extracted from BigQuery' % source)
        return df

    def extract(self):
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
                elif self.sources[source]['type'] == 'bq':
                    self.extracted[source] = self.extract_via_bq(
                        source, self.sources[source])

    def transform(self):
        for source in self.sources:
            if not self.args.source or self.args.source == source:
                config = self.sources[source]
                transform_method = getattr(self, 'transform_{}'.format(source))
                self.transformed[source] = transform_method(source, config)
                print('%s transformed' % source)

    def load_to_fs(self, source, config, stage='raw'):
        fpath = self.get_or_create_filepath(source, config, stage, 'fs')
        if stage == 'raw':
            # write multiple raw files if paged
            raw = self.raw[source]
            if isinstance(raw, list):
                for i, r in enumerate(raw):
                    fpath = self.get_or_create_filepath(
                        source, config, stage, 'fs', i+1)
                    with open(fpath, 'w') as f:
                        f.write(r)
                print('%s x %d pages loaded to file system.' % (source, len(raw)))
            else:
                with open(fpath, 'w') as f:
                    f.write(raw)
                    print('%s loaded to file system.' % source)
        else:
            with open(fpath, 'w') as f:
                output = ''
                if self.destinations['fs']['file_format'] == 'json':
                    output = self.transformed[source].to_json()
                elif self.destinations['fs']['file_format'] == 'csv':
                    output = self.transformed[source].to_csv()
                f.write(output)
                print('%s loaded to file system.' % source)

    def load_to_gcs(self, source, config, stage='raw'):
        bucket = self.gcs.bucket(self.destinations['gcs']['bucket'])
        blob = bucket.blob(self.get_filepath(source, config, stage, 'gcs'))
        blob.upload_from_filename(self.get_filepath(source, config, stage, 'fs'))
        print('%s loaded to GCS.' % source)

    def load(self):
        for source in self.sources:
            if not self.args.source or self.args.source == source:
                config = self.sources[source]
                self.load_to_fs(source, config, self.stage)
                if self.args.dest != 'fs':
                    self.load_to_gcs(source, config, self.stage)

    def run(self):
        if self.args.step and self.args.step[0].upper() not in ['E', 'T', 'L']:
            raise ValueError('Invalid argument specified.')
        if not self.args.step or self.args.step[0].upper() in ['E', 'T', 'L']:
            self.extract()
        if not self.args.step or self.args.step[0].upper() in ['T', 'L']:
            self.transform()
        if not self.args.step or self.args.step[0].upper() in ['L']:
            self.load()
