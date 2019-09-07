import sys

import pytest

import etl


@pytest.mark.intgtest
def test_etl():
    sys.argv = ["./etl.py", "--debug"]
    etl.main()
