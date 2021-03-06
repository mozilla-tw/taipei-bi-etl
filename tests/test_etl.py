import sys

import etl
import pytest


@pytest.mark.unittest
def test_rps__global_package__fs():
    sys.argv = [
        "./etl.py",
        "--debug",
        "--config=test",
        "--task=rps",
        "--step=e",
        "--source=global_package",
        "--dest=fs",
    ]
    etl.main()
