#!/usr/bin/env python3


"""
This script loads market data from various sources and combine them into one big file.
"""
from tasks import base, revenue


def main(args):
    if args.task == 'revenue':
        revenue.main(args)


if __name__ == "__main__":
    arg_parser = base.get_arg_parser()
    main(arg_parser.parse_args())
