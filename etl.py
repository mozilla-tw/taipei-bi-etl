#!/usr/bin/env python3


"""
This script loads market data from various sources and combine them into one big file.
"""
import os
import os.path
import glob
from tasks import base, revenue

arg_parser = base.get_arg_parser()


def main(args):
    os.system('gcloud config list')
    if args.rm:
        files = glob.glob('./data/*')
        for f in files:
            os.remove(f)
        print("Clean up cached files")
    if args.task == 'revenue':
        revenue.main(args)


if __name__ == "__main__":
    main(arg_parser.parse_args())
