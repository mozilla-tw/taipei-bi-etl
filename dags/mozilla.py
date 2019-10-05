"""Mozilla dag."""
import datetime

from google.cloud import bigquery

from airflow import models
from airflow.operators.python_operator import PythonOperator

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

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
    """Callable Function."""
    pass


def func_rawEvent(**context):
    """Callable Function."""
    pass


def func_unnestEvent(**context):
    """Callable Function."""
    pass


def func_channelRFELatestView(**context):
    """Callable Function."""
    pass


def func_channelRFE(**context):
    """Callable Function."""
    pass


def func_featureRFELatestView(**context):
    """Callable Function."""
    pass


def func_featureRFE(**context):
    """Callable Function."""
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
