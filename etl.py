#!/usr/bin/env python3
"""This is the entry script for all ETL tasks.

It will run task specified in args, or if not specified,
run all tasks in the correct sequence.
"""
import utils.config
from tasks import rps, revenue, bigquery, adjust
import logging as log


def main():
    """Determine which task to run based on args.task."""
    arg_parser = utils.config.get_arg_parser()
    args = arg_parser.parse_args()
    if args.loglevel:
        log.basicConfig(level=log.getLevelName(args.loglevel))
    if args.debug:
        if args.loglevel is None:
            log.basicConfig(level=log.DEBUG)
    task = None
    if args.task:
        if args.task == "bigquery":
            task = bigquery
        elif args.task == "rps":
            task = rps
        elif args.task == "revenue":
            task = revenue
        elif args.task == "adjust":
            task = adjust
        else:
            assert False, "Invalid task name %s" % args.task
    if task:
        arg_parser = utils.config.get_arg_parser(**task.DEFAULTS)
        task.main(arg_parser.parse_args())
    else:  # Run all tasks in sequence
        # rps.main(utils.config.get_arg_parser(**rps.DEFAULTS).parse_args())
        revenue.main(utils.config.get_arg_parser(**revenue.DEFAULTS).parse_args())
        adjust.main(utils.config.get_arg_parser(**adjust.DEFAULTS).parse_args())
        bigquery.main(utils.config.get_arg_parser(**bigquery.DEFAULTS).parse_args())


if __name__ == "__main__":
    main()
