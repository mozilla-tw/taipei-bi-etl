import datetime
import logging

import pytest
from google.cloud import bigquery

import tasks.bigquery
import utils.config

log = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def client():
    return bigquery.Client()


@pytest.fixture
def always_latest(monkeypatch):
    def always_true(self):
        return True

    # does it work with imported class?
    monkeypatch.setattr(tasks.bigquery.BqTask, "is_latest", always_true)


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
    SELECT_TABLE = utils.config.get_configs("bigquery", "test").SELECT_TABLE

    SELECT_TABLE["params"]["project"] = client.project
    dataset_name = SELECT_TABLE["params"]["dataset"]
    dest_name = SELECT_TABLE["params"]["dest"]

    # Now default location is US. Not configurable.
    # https://github.com/googleapis/google-cloud-python/blob/bigquery-1.17.0/bigquery/samples/create_dataset.py#L31
    dataset = client.create_dataset(dataset_name)
    to_delete.extend([dataset])

    log.debug(
        "Create table %s.%s by selecting rows from public table."
        % (dataset_name, dest_name)
    )
    task = tasks.bigquery.get_task(SELECT_TABLE, datetime.datetime(2005, 7, 14, 12, 30))
    task.daily_run()

    table = client.get_table("%s.%s" % (dataset_name, dest_name))

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
    MANGO_EVENTS = utils.config.get_configs(args.task, args.config).MANGO_EVENTS
    dataset_name = MANGO_EVENTS["params"]["dataset"]
    dest_name = MANGO_EVENTS["params"]["dest"]

    dataset = client.create_dataset(dataset_name)
    to_delete.extend([dataset])

    tasks.bigquery.main(args)

    table = client.get_table("%s.%s" % (dataset_name, dest_name))

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
            "--date",
            "2019-09-26",
        ]
    )
    MANGO_EVENTS_UNNESTED = utils.config.get_configs(
        args.task, args.config
    ).MANGO_EVENTS_UNNESTED

    dataset_name = MANGO_EVENTS_UNNESTED["params"]["dataset"]
    dest_name = MANGO_EVENTS_UNNESTED["params"]["dest"]

    tasks.bigquery.main(args)

    view = client.get_table("%s.%s" % (dataset_name, dest_name))

    view_query = view.view_query
    assert isinstance(view_query, str) and len(view_query) != 0

    # run second time to update the bigquery view,
    # check udf replace functionality.
    tasks.bigquery.main(args)

    view = client.get_table("%s.%s" % (dataset_name, dest_name))

    view_query = view.view_query
    assert isinstance(view_query, str) and len(view_query) != 0


@pytest.mark.intgtest
def test_channel_mapping_truncate(client, to_delete, always_latest):
    arg_parser = utils.config.get_arg_parser()

    # testing mango_events
    args = arg_parser.parse_args(
        [
            "--config",
            "test",
            "--task",
            "bigquery",
            "--subtask",
            "mango_channel_mapping",
            "--date",
            "2019-10-26",
        ]
    )
    config = utils.config.get_configs(args.task, args.config).MANGO_CHANNEL_MAPPING
    dataset_name = config["params"]["dataset"]
    dest_name = config["params"]["dest"]

    dataset = client.create_dataset(dataset_name)
    to_delete.extend([dataset])

    tasks.bigquery.main(args)

    table = client.get_table("%s.%s" % (dataset_name, dest_name))

    assert table.num_rows != 0
    # a physical table is not view, should not have query string
    assert table.view_query is None


@pytest.mark.todo
def test_channel_mapping_schema_change(client, to_delete, always_latest):
    arg_parser = utils.config.get_arg_parser()

    # Test schema change.
    # regression test.
    dates = ["2019-10-03", "2019-10-26"]
    for idx, date in enumerate(dates):
        args = arg_parser.parse_args(
            [
                "--config",
                "test",
                "--task",
                "bigquery",
                "--subtask",
                "mango_channel_mapping",
                "--date",
                date,
            ]
        )

        config = utils.config.get_configs(args.task, args.config).MANGO_CHANNEL_MAPPING

        if idx == 0:
            # For the first case, create the dataset before any operation.
            dataset_name = config["params"]["dataset"]
            dest_name = config["params"]["dest"]

            dataset = client.create_dataset(dataset_name)
            to_delete.extend([dataset])

        tasks.bigquery.main(args)

        table = client.get_table("%s.%s" % (dataset_name, dest_name))

        assert table.num_rows != 0
        # a physical table is not view, should not have query string
        assert table.view_query is None


@pytest.mark.unittest
def test_next_execution_date(client, to_delete):
    arg_parser = utils.config.get_arg_parser()

    args = arg_parser.parse_args(  # noqa: F841
        [
            "--config",
            "test",
            "--task",
            "bigquery",
            "--subtask",
            "mango_channel_mapping",
            "--date",
            "2019-11-03",
            "--next_execution_date",
            "2019-11-04 00:00:00+00:00",
        ]
    )

    # TODO: test parameter parsing for next_execution_date
