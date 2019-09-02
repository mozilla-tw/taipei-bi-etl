"""Shared pytest fixtures."""
import datetime
import pytest
from configs import rps as rps_cfg
from configs.debug import rps as rps_dbg_cfg
from tasks import rps, base


@pytest.fixture(
    params=[v for k, v in rps_cfg.SOURCES.items() if v["type"] == "api"],
    ids=[k for k, v in rps_cfg.SOURCES.items() if v["type"] == "api"],
)
def api_src(request):
    """Pytest fixture for accessing source API configs."""
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
    """Pytest fixture for accessing destination GCS configs."""
    return request.param


@pytest.fixture(scope="module")
def req():
    """Pytest fixture for accessing requests library."""
    import requests

    return requests


@pytest.fixture(scope="module")
def gcs():
    """Pytest fixture for accessing GCS library."""
    from google.cloud import storage

    return storage.Client()


@pytest.fixture
def gcs_bucket(gcs, gcs_dest):
    """Pytest fixture for accessing GCS bucket."""
    bucket = gcs.bucket(gcs_dest["bucket"])
    return bucket
