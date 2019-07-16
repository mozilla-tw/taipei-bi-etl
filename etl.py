#!/usr/bin/env python3


"""
This script loads market data from various sources and combine them into one big file.
"""
from argparse import ArgumentParser
import os,sys
import os.path
import requests
import time
import csv
import glob
import settings
import datetime

parser = ArgumentParser(description=__doc__)
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

def extract(d, p):
    sources = settings.SOURCES
    current_date = d
    if None is current_date:
        current_date = datetime.datetime.today()
    else:
        current_date = datetime.datetime.strptime(d, '%Y-%m-%d')
    if None is p:
        p = 30
    else:
        p = int(p)

    prev_month = current_date - datetime.timedelta(days=p)
    for name, source in sources.items():
        fpath = './data/{}_latest.json'.format(name)
        if not os.path.isfile(fpath):
            url = source['url'].format(api_key=source['api_key'], start_date=prev_month.strftime(source['date_format']), end_date=current_date.strftime(source['date_format']))
            print(url)
            r = requests.get(url, allow_redirects=True)
            open(fpath, 'wb').write(r.content)
            print('{} saved'.format(fpath))
            time.sleep(10)
        else:
            print('{} exists'.format(fpath))


def main():
    args = parser.parse_args()
    print(args)
    if args.rm == True:
        files = glob.glob('./data/*')
        for f in files:
            os.remove(f)
        print("Clean up cached files")
    extract(args.date, args.period)
    #transform()

if __name__ == "__main__":
    main()
