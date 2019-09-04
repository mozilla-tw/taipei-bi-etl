import sys

import pytest

import etl


@pytest.mark.intgtest
def test_etl():
    sys.argv = ["--debug"]
    etl.main()
