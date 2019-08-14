from argparse import ArgumentParser
import os
import os.path
import requests
import datetime
from google.cloud import bigquery


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
        self.dependencies = dependencies
        self.extracted = dict()
        self.bq = bigquery.Client()

    def extract_via_api(self, dependency, config):
        current_date = self.date
        if None is current_date:
            current_date = datetime.datetime.today()
        else:
            current_date = datetime.datetime.strptime(self.date, '%Y-%m-%d')
        if None is self.period:
            self.period = 30
        else:
            self.period = int(self.period)

        prev_month = current_date - datetime.timedelta(days=self.period)
        fpath = './data/{}_latest.json'.format(dependency)
        if not os.path.isfile(fpath):
            url = config['url'].format(api_key=config['api_key'],
                                       start_date=prev_month.strftime(config['date_format']),
                                       end_date=current_date.strftime(config['date_format']))
            print(url)
            r = requests.get(url, allow_redirects=True)
            extracted = r.content
            with open(fpath, 'wb') as f:
                f.write(r.content)
                print('{} saved'.format(fpath))
        else:
            print('{} exists'.format(fpath))
            with open(fpath, 'r') as f:
                extracted = f.read()
        return extracted

    def extract_via_bq(self, dependency, config):
        print('Implement BigQuery data extraction here.')
        return False

    def extract(self):
        for dependency in self.dependencies:
            if self.dependencies[dependency]['type'] == 'api':
                self.extracted[dependency] = self.extract_via_api(dependency, self.dependencies[dependency])
            elif self.dependencies[dependency]['type'] == 'bq':
                self.extracted[dependency] = self.extract_via_bq(dependency, self.dependencies[dependency])

    def transform(self):
        print('Transform data here.')

    def load_to_gcs(self):
        print('Load data to GCS here.')

    def load(self):
        self.load_to_gcs()
