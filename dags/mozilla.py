import csv
import datetime
import inspect
import io
import logging
import time

from google.cloud import bigquery

from airflow import models
from airflow.operators import dummy_operator
from airflow.operators.python_operator import PythonOperator

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

try:
    import importlib.resources as pkg_resources
except ImportError:
    # Try backported to PY<37 `importlib_resources`.
    import importlib_resources as pkg_resources


default_args = {
    "owner": "airflow",
    "start_date": yesterday,
    "depends_on_past": True,
    "email": [""],
    "provide_context": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=5),
}

client = bigquery.Client()


def func_drop(**context):
    pass


def func_rawEvent(**context):
    pass


def func_unnestEvent(**context):
    pass


def func_channelRFELatestView(**context):
    pass


def func_channelRFE(**context):
    pass


def func_featureRFELatestView(**context):
    pass


def func_featureRFE(**context):
    pass


with models.DAG("mozilla", default_args=default_args, schedule_interval=None) as dag:
    rawEvent = PythonOperator(
        task_id="rawEvent", provide_context=True, python_callable=func_rawEvent, dag=dag
    )
    unnestEvent = PythonOperator(
        task_id="unnestEvent",
        provide_context=True,
        python_callable=func_unnestEvent,
        dag=dag,
    )
    channelRFELatestView = PythonOperator(
        task_id="channelRFELatestView",
        provide_context=True,
        python_callable=func_channelRFELatestView,
        dag=dag,
    )
    channelRFE = PythonOperator(
        task_id="channelRFE",
        provide_context=True,
        python_callable=func_channelRFE,
        dag=dag,
    )
    featureRFELatestView = PythonOperator(
        task_id="featureRFELatestView",
        provide_context=True,
        python_callable=func_featureRFELatestView,
        dag=dag,
    )
    featureRFE = PythonOperator(
        task_id="featureRFE",
        provide_context=True,
        python_callable=func_featureRFE,
        dag=dag,
    )

    drop = PythonOperator(
        task_id="drop", provide_context=True, python_callable=func_drop, dag=dag
    )

    drop >> rawEvent
    rawEvent >> unnestEvent
    unnestEvent >> channelRFELatestView >> channelRFE
    unnestEvent >> featureRFELatestView >> featureRFE
