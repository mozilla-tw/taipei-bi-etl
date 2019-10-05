"""Testing DAG."""
import os
import os.path
import sys

import pytest

from airflow import models

from . import unit_testing


@pytest.mark.unittest
def test_mozilla():
    """Test that the DAG file can be successfully imported."""
    from dags import mozilla

    unit_testing.assert_has_valid_dag(mozilla)
