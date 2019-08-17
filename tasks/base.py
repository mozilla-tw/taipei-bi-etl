from argparse import ArgumentParser
import os
import os.path
import requests
import datetime
import pandas_gbq as pdbq
from google.cloud import storage


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
        self.stage = stage
        self.args = args
        self.period = args.period
        self.current_date = args.date
        self.last_month = args.date
        self.init_dates()
        self.sources = sources
        self.destinations = destinations
        self.extracted = dict()
        self.transformed = dict()
        self.gcs = storage.Client()

    def init_dates(self):
        self.last_month = self.current_date - datetime.timedelta(days=self.period)

    def get_filepath(self, source, config, stage, dest):
        return '{}{}'.format(self.destinations[dest]['prefix'],
                             self.get_filename(source, config, stage))

    def get_filename(self, source, config, stage):
        ftype = 'json' if 'load_type' not in config else config['load_type']
        return '{stage}-{task}-{source}-{date}.{ext}'.format(
            stage=stage, task=self.args.task, source=source,
            date=self.current_date.strftime(DEFAULT_DATE_FORMAT), ext=ftype)

    def is_cached(self, source, config):
        fpath = self.get_filepath(source, config, 'raw', 'fs')
        return os.path.isfile(fpath)

    def extract_via_fs(self, source, config, stage='raw'):
        fpath = self.get_filepath(source, config, stage, 'fs')
        with open(fpath, 'r') as f:
            extracted = f.read()
        print('%s extracted from file system' % source)
        return extracted

    def extract_via_api(self, source, config):

        url = config['url'].format(api_key=config['api_key'],
                                   start_date=self.last_month.strftime(
                                       config['date_format']),
                                   end_date=self.current_date.strftime(
                                       config['date_format']))
        r = requests.get(url, allow_redirects=True)
        extracted = r.content
        print('%s extracted from API' % source)
        return extracted

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
        fpath = self.get_filepath(source, config, stage, 'fs')
        with open(fpath, 'wb') as f:
            f.write(self.extracted[source])
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
