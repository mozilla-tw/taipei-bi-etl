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

    configs.SELECT_TABLE["id"]["project"] = client.project

    # Now default location is US. Not configurable.
    # https://github.com/googleapis/google-cloud-python/blob/bigquery-1.17.0/bigquery/samples/create_dataset.py#L31
    dataset = client.create_dataset(configs.SELECT_TABLE["id"]["dataset"])
    to_delete.extend([dataset])

    task = tasks.bigquery.get_task_by_config(
        configs.SELECT_TABLE, datetime.datetime(2005, 7, 14, 12, 30)
    )
    task.daily_run()

    table = client.get_table(
        "%s.%s"
        % (
            configs.SELECT_TABLE["id"]["dataset"],
            configs.SELECT_TABLE["params"]["dest"],
        )
    )

    assert table.num_rows != 0
