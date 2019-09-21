"""Shared utilities for tests."""
import datetime
from typing import Dict, Callable, Any, Tuple
from _pytest.fixtures import FixtureRequest
import utils.config
import logging
import pytest

from utils.query import build_query

log = logging.getLogger(__name__)


def inject_fixtures(namespace: Dict[str, Any], task: str, cfgs: Dict[str, Any]):
    """Inject fixtures for namespace.

    :param namespace: the `globals()` namespace of the module to be injected.
    :param task: the name of the ETL task.
    :param cfgs: configs of the ETL task.
    """
    fixtures = generate_fixtures(task, cfgs)
    for name, fixture in fixtures.items():
        namespace[name] = fixture


def get_default_range(request: FixtureRequest) -> Tuple[str, str]:
    """Get default data range."""
    date = datetime.datetime.now()
    period = 30
    markers = [x.name for x in request.node.iter_markers()]
    if "rps" in markers:
        from tasks import rps

        date = rps.DEFAULTS["date"] if "date" in rps.DEFAULTS else date
        period = rps.DEFAULTS["period"] if "period" in rps.DEFAULTS else period
    elif "revenue" in markers:
        from tasks import revenue

        date = revenue.DEFAULTS["date"] if "date" in revenue.DEFAULTS else date
        period = revenue.DEFAULTS["period"] if "period" in revenue.DEFAULTS else period
    assert date is not None
    assert period is not None
    ed = date
    sd = ed - datetime.timedelta(days=period)
    ed = ed.strftime(utils.config.DEFAULT_DATE_FORMAT)
    sd = sd.strftime(utils.config.DEFAULT_DATE_FORMAT)
    return sd, ed


def get_src_by_type(task: str, cfgs: Dict[str, Any], t: str) -> Dict[str, Any]:
    """Get source config by type."""
    params = []
    ids = []
    for src, cfg in cfgs.items():
        if cfg is None:
            continue
        params += [v for k, v in cfg.SOURCES.items() if v["type"] == t]
        ids += [
            task + "_" + src + "_" + k for k, v in cfg.SOURCES.items() if v["type"] == t
        ]
    return {"params": params, "ids": ids, "scope": "module"}


def get_dest_by_type(task: str, cfgs: Dict[str, Any], t: str) -> Dict[str, Any]:
    """Get destination config by type."""
    params = []
    ids = []
    for src, cfg in cfgs.items():
        if cfg is None:
            continue
        params += [v for k, v in cfg.DESTINATIONS.items() if k == t]
        ids += [
            task + "_" + src + "_" + k for k, v in cfg.DESTINATIONS.items() if k == t
        ]
    return {"params": params, "ids": ids, "scope": "module"}


def generate_fixtures(task: str, cfgs: Dict[str, Any]) -> Dict[str, Callable]:
    """Generate fixtures based on task configs.

    :rtype: dict
    :param task: The task to run tests on
    :param cfgs: Configs of the task.
    :return: a dictionary of fixtures
    """
    fixtures = {}
    api_src_cfg = get_src_by_type(task, cfgs, "api")
    if len(api_src_cfg["params"]) > 0:

        @pytest.fixture(**api_src_cfg)
        def api_src(request):
            """Pytest fixture for accessing source API configs."""
            sd, ed = get_default_range(request)
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
            return request.param

        fixtures["api_src"] = api_src

    gcs_src_cfg = get_src_by_type(task, cfgs, "gcs")
    if len(gcs_src_cfg["params"]) > 0:

        @pytest.fixture(**gcs_src_cfg)
        def gcs_src(request):
            """Pytest fixture for accessing source GCS configs."""
            return request.param

        fixtures["gcs_src"] = gcs_src

    bq_src_cfg = get_src_by_type(task, cfgs, "bq")
    if len(bq_src_cfg["params"]) > 0:

        @pytest.fixture(**bq_src_cfg)
        def bq_src(request):
            """Pytest fixture for accessing source BQ configs."""
            sd, ed = get_default_range(request)
            cfg = request.param
            cfg["sql"] = build_query(cfg, sd, ed)
            return request.param

        fixtures["bq_src"] = bq_src

    gcs_dest_cfg = get_dest_by_type(task, cfgs, "gcs")
    if len(gcs_dest_cfg["params"]) > 0:

        @pytest.fixture(**gcs_dest_cfg)
        def gcs_dest(request):
            """Pytest fixture for accessing destination GCS configs."""
            return request.param

        fixtures["gcs_dest"] = gcs_dest

        @pytest.fixture(scope="module")
        def gcs_bucket(gcs, gcs_dest):
            """Pytest fixture for accessing GCS bucket."""
            bucket = gcs.bucket(gcs_dest["bucket"])
            return bucket

        fixtures["gcs_bucket"] = gcs_bucket

    return fixtures
