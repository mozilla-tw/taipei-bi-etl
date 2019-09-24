"""Test file utils."""
import os
import tempfile

import pytest

from utils.file import read_string, write_string


def _create_temp_file(name="", suffix="") -> str:
    if not name:
        name = tempfile.template
    file_name = tempfile.NamedTemporaryFile(
        delete=False, prefix=name, suffix=suffix
    ).name
    return file_name


@pytest.mark.unittest
def test_write_string():
    STR = "test1"
    fname = _create_temp_file()
    write_string(fname, STR)
    with open(fname, "r") as f:
        data = f.read()
    assert data == STR
    os.remove(fname)


@pytest.mark.unittest
def test_read_string():
    STR = "test1"
    fname = _create_temp_file()
    with open(fname, "w") as f:
        f.write(STR)
    data = read_string(fname)
    assert data == STR
    os.remove(fname)
