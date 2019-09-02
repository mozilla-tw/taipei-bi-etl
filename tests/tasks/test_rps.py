import datetime
import time
import pytest
from tasks import base, rps
from configs import rps as rps_cfg
from configs.debug import rps as rps_dbg_cfg
import logging

log = logging.getLogger(__name__)


@pytest.fixture(
    params=[v for k, v in rps_cfg.SOURCES.items() if v["type"] == "api"],
    ids=[k for k, v in rps_cfg.SOURCES.items() if v["type"] == "api"],
)
def api_src(request):
    """A pytest fixture for accessing source API configs."""
    ed = rps.DEFAULTS["date"]
    sd = ed - datetime.timedelta(days=rps.DEFAULTS["period"])
    ed = ed.strftime(base.DEFAULT_DATE_FORMAT)
    sd = sd.strftime(base.DEFAULT_DATE_FORMAT)
    cfg = request.param
    if "iterator" in cfg:
        cfg["url"] = cfg["url"].format(
            api_key=cfg["api_key"],
            start_date=sd,
            end_date=ed,
            iterator=cfg["iterator"][0],
        )
    elif "page_size" in cfg:
        cfg["url"] = cfg["url"].format(
            api_key=cfg["api_key"],
            start_date=sd,
            end_date=ed,
            page=1,
            limit=cfg["page_size"],
        )
    else:
        cfg["url"] = cfg["url"].format(
            api_key=cfg["api_key"], start_date=sd, end_date=ed
        )
    return cfg


@pytest.fixture(
    params=(
        [v for k, v in rps_cfg.DESTINATIONS.items() if k == "gcs"]
        + [v for k, v in rps_dbg_cfg.DESTINATIONS.items() if k == "gcs"]
    ),
    ids=(
        [k for k, v in rps_cfg.DESTINATIONS.items() if k == "gcs"]
        + ["dbg_" + k for k, v in rps_dbg_cfg.DESTINATIONS.items() if k == "gcs"]
    ),
)
def gcs_dest(request):
    """A pytest fixture for accessing destination GCS configs."""
    return request.param


@pytest.fixture(scope="module")
def req():
    """A pytest fixture for accessing requests library."""
    import requests

    return requests


@pytest.fixture(scope="module")
def gcs():
    """A pytest fixture for accessing GCS library."""
    from google.cloud import storage

    return storage.Client()


@pytest.fixture
def gcs_bucket(gcs, gcs_dest):
    """A pytest fixture for accessing GCS bucket."""
    bucket = gcs.bucket(gcs_dest["bucket"])
    return bucket


def test_read_api(req, api_src):
    """Test calling APIs in source configs."""
    time.sleep(1)
    r = req.get(api_src["url"], allow_redirects=True)
    assert len(r.text) > 0


def test_write_gcs(gcs_bucket, gcs_dest):
    """Test writing a GCS blob in destination config."""
    blob = gcs_bucket.blob(gcs_dest["prefix"] + "test.txt")
    blob.upload_from_string("This is a test.")
    blob.delete()
