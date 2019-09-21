"""File utilities."""
import re

from utils.config import EXT_REGEX, DEFAULT_PATH_FORMAT


def write_string(path: str, s: str):
    """Wrapper function to write string to file.

    :param path: the file path to write to
    :param s: the string to write
    """
    with open(path, "w") as f:
        f.write(s)


def read_string(path: str) -> str:
    """Wrapper function to read string from file.

    :param path: the file path to read from
    :return: the string read from the file
    """
    with open(path, "r") as f:
        s = f.read()
    return s


def get_file_ext(fpath: str) -> str:
    """Extract file extension from path.

    :rtype: str
    :param fpath: the path to extract
    :return: the extracted file extension

    >>> get_file_ext("gs://some-bucket/some-path/some-name.json")
    'json'
    """
    return re.search(EXT_REGEX, fpath).group(1)[1:]


def get_path_prefix(fpath: str) -> str:
    """Extract prefix from path.

    Now only used for loading cached files from GCS.

    :rtype: str
    :param fpath: the path to extract
    :return: the extracted prefix

    >>> get_path_prefix("some-name.*.json")
    'some-name.'
    >>> get_path_prefix("some-name.json")
    'some-name.'
    """
    ext_search = re.search(EXT_REGEX, fpath)
    return fpath[: ext_search.start() + 1]


def get_path_format(wildcard: bool = False) -> str:
    """Get the format string of file paths.

    :rtype: str
    :param wildcard: whether it's a wildcard path or not.
    :return: the path format string

    >>> get_path_format()
    '{prefix}{stage}-{task}-{source}/{filename}'
    >>> get_path_format(True)
    '{prefix}{stage}-{task}-{source}/*'
    """
    if wildcard:
        return DEFAULT_PATH_FORMAT + "/*"
    else:
        return DEFAULT_PATH_FORMAT + "/{filename}"
