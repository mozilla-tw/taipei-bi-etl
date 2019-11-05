import datetime
import logging
from argparse import Namespace

import pytest
from google.cloud import storage
from pandas import DataFrame

import tasks.adjust
import utils.config

task = "adjust"
cfg = utils.config.get_configs(task, "test")

log = logging.getLogger(__name__)


@pytest.fixture
def load_assets(mock_requests):
    source = "adjust_trackers"
    dates = ["2019-09-26", "2019-09-27"]

    for date in dates:
        URL = cfg.SOURCES[source]["url"].format(  # pylint: disable=no-member
            api_key=cfg.SOURCES[source]["api_key"], end_date=date
        )
        with open("test-data/raw-adjust-adjust_trackers/%s.json" % date, "r") as f:
            CONTENT = f.read()
        mock_requests.setContent(URL, CONTENT)


@pytest.mark.unittest
def test_adjust_extract(load_assets):
    args = Namespace(
        config="test",
        date=datetime.datetime(2019, 9, 26, 0, 0),
        debug=True,
        rm=False,
        source="adjust_trackers",
        # loglevel=None,
        period=30,
        step="e",
        task="adjust",
        dest=None,
    )

    task = tasks.adjust.AdjustEtlTask(args, cfg.SOURCES, cfg.SCHEMA, cfg.DESTINATIONS)
    task.extract()
    df = task.extracted[args.source]  # pylint: disable=no-member

    log.debug("shape of df: %s" % str(df.shape))
    assert isinstance(df, DataFrame)
    assert len(df.index) != 0


@pytest.mark.unittest
def test_adjust(load_assets, mock_gcs):
    arg_parser = utils.config.get_arg_parser()

    gcs = storage.Client()
    log.debug("bucket: %s" % cfg.DESTINATIONS["gcs"]["bucket"])
    bucket = gcs.create_bucket(cfg.DESTINATIONS["gcs"]["bucket"])

    dates = ["2019-09-26", "2019-09-27"]
    for idx, date in enumerate(dates):
        args = arg_parser.parse_args(
            ["--config", "test", "--task", "adjust", "--date", date]
        )

        assert len([item for item in bucket.list_blobs()]) == idx

        tasks.adjust.main(args)

        # TODO: check filename
        assert len([item for item in bucket.list_blobs()]) == idx + 1
