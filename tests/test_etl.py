import sys

import pytest

import etl


@pytest.mark.intgtest
def test_etl():
    sys.argv = ["./etl.py", "--debug"]
    etl.main()

@pytest.mark.unittest
def test_rps__global_package__fs():
    sys.argv = ["./etl.py", "--debug", "--config=test", "--task=rps", "--step=e", "--source=global_package", "--dest=fs"]
    etl.main()

