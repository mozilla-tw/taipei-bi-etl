import datetime

import pytest
from google.cloud import bigquery

import tasks.bigquery
import utils.config


@pytest.fixture(scope="module")
def client():
    return bigquery.Client()


@pytest.fixture
def to_delete(client):
    doomed = []
    yield doomed
    for item in doomed:
        if isinstance(item, (bigquery.Dataset, bigquery.DatasetReference)):
            client.delete_dataset(item, delete_contents=True)
        elif isinstance(item, (bigquery.Table, bigquery.TableReference)):
            client.delete_table(item)
        else:
            item.delete()


@pytest.mark.intgtest
def test_BqTableTask(client, to_delete):
    configs = utils.config.get_configs("bigquery", "test")

    configs.SELECT_TABLE["params"]["project"] = client.project

    # Now default location is US. Not configurable.
    # https://github.com/googleapis/google-cloud-python/blob/bigquery-1.17.0/bigquery/samples/create_dataset.py#L31
    dataset = client.create_dataset(configs.SELECT_TABLE["params"]["dataset"])
    to_delete.extend([dataset])

    task = tasks.bigquery.get_task(
        configs.SELECT_TABLE, datetime.datetime(2005, 7, 14, 12, 30)
    )
    task.daily_run()

    table = client.get_table(
        "%s.%s"
        % (
            configs.SELECT_TABLE["params"]["dataset"],
            configs.SELECT_TABLE["params"]["dest"],
        )
    )

    assert table.num_rows != 0


@pytest.mark.intgtest
def test_mango_events(client, to_delete):
    arg_parser = utils.config.get_arg_parser()

    # testing mango_events
    args = arg_parser.parse_args(
        [
            "--config",
            "test",
            "--task",
            "bigquery",
            "--subtask",
            "mango_events",
            "--date",
            "2019-09-26",
        ]
    )
    configs = utils.config.get_configs(args.task, args.config)

    dataset = client.create_dataset(configs.MANGO_EVENTS["id"]["dataset"])
    to_delete.extend([dataset])

    tasks.bigquery.main(args)

    table = client.get_table(
        "%s.%s"
        % (
            configs.MANGO_EVENTS["id"]["dataset"],
            configs.MANGO_EVENTS["params"]["dest"],
        )
    )

    assert table.num_rows != 0
    # a physical table is not view, should not have query string
    assert table.view_query is None

    # testing mango_events_unnested
    args = arg_parser.parse_args(
        [
            "--config",
            "test",
            "--task",
            "bigquery",
            "--subtask",
            "mango_events_unnested",
            "--createschema",
            "--date",
            "2019-09-26",
        ]
    )
    configs = utils.config.get_configs(args.task, args.config)

    tasks.bigquery.main(args)

    view = client.get_table(
        "%s.%s"
        % (
            configs.MANGO_EVENTS_UNNESTED["id"]["dataset"],
            configs.MANGO_EVENTS_UNNESTED["params"]["dest"],
        )
    )

    view_query = view.view_query
    assert isinstance(view_query, str) and len(view_query) != 0
