#!/usr/bin/env python3
"""This is the entry script for all ETL tasks.

It will run task specified in args, or if not specified,
run all tasks in the correct sequence.
"""
from tasks import base, rps, revenue  # , rfe
import logging as log


def main():
    """Determine which task to run based on args.task."""
    arg_parser = base.get_arg_parser()
    args = arg_parser.parse_args()
    if args.debug:
        log.basicConfig(level=log.DEBUG)
    task = None
    if args.task == "rps":
        task = rps
    if args.task == "revenue":
        task = revenue
    # if args.task == 'rfe':
    #     task = rfe
    if task:
        arg_parser = base.get_arg_parser(**task.DEFAULTS)
        task.main(arg_parser.parse_args())
    else:  # Run all tasks in sequence
        rps.main(base.get_arg_parser(**rps.DEFAULTS).parse_args())
        revenue.main(base.get_arg_parser(**revenue.DEFAULTS).parse_args())


if __name__ == "__main__":
    main()
