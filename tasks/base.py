from argparse import ArgumentParser
import os
import os.path
import requests
import datetime
import pandas_gbq as pdbq
import pandas as pd


def get_arg_parser():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--task",
        default=None,
        help="The ETL task to run.",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="The base (latest) date of the data.",
    )
    parser.add_argument(
        "--period",
        default=None,
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

    def __init__(self, date, period, dependencies):
        self.date = date
        self.period = period
        self.current_date = date
        self.last_month = date
        self.init_dates()
        self.dependencies = dependencies
        self.extracted = dict()

    def init_dates(self):
        if None is self.current_date:
            self.current_date = datetime.datetime.today()
        else:
            self.current_date = datetime.datetime.strptime(self.date, '%Y-%m-%d')
        if None is self.period:
            self.period = 30
        else:
            self.period = int(self.period)
        self.last_month = self.current_date - datetime.timedelta(days=self.period)


    def is_cached(self, dependency, config):
        fpath = './data/{}_latest.json'.format(dependency)
        return os.path.isfile(fpath)

    def extract_via_filesystem(self, dependency, config):
        fpath = './data/{}_latest.json'.format(dependency)
        with open(fpath, 'r') as f:
            extracted = f.read()
        return extracted

    def extract_via_api(self, dependency, config):

        url = config['url'].format(api_key=config['api_key'],
                                   start_date=self.last_month.strftime(config['date_format']),
                                   end_date=self.current_date.strftime(config['date_format']))
        r = requests.get(url, allow_redirects=True)
        extracted = r.content

        return extracted

    def extract_via_bq(self, dependency, config):
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
                query += f.read().format(from_date=self.last_month.strftime(config['date_format']))
        df = pdbq.read_gbq(query)
        return df

    def extract(self):
        for dependency in self.dependencies:
            if self.dependencies[dependency]['type'] == 'api':
                config = self.dependencies[dependency]
                if 'cache_file' in config and config['cache_file']:
                    if not self.is_cached(dependency, config):
                        self.extracted[dependency] = self.extract_via_api(dependency, config)
                        self.load_to_filesystem(dependency, config)
                    else:
                        self.extracted[dependency] = self.extract_via_filesystem(dependency, config)
                else:
                    self.extracted[dependency] = self.extract_via_api(dependency, config)
            elif self.dependencies[dependency]['type'] == 'bq':
                self.extracted[dependency] = self.extract_via_bq(dependency, self.dependencies[dependency])

    def transform(self):
        print('Transform data here.')

    def load_to_filesystem(self, dependency, config):
        fpath = './data/{}_latest.json'.format(dependency)
        with open(fpath, 'wb') as f:
            f.write(self.extracted[dependency])
            print('{} saved'.format(fpath))

    def load_to_gcs(self):
        print('Load data to GCS here.')

    def load(self):
        self.load_to_gcs()
