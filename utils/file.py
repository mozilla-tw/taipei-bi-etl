"""File utilities."""
import re

from utils.config import EXT_REGEX, DEFAULT_PATH_FORMAT


def get_file_ext(fpath: str) -> str:
    """Extract file extension from path.

    :rtype: str
    :param fpath: the path to extract
    :return: the extracted file extension
    """
    return re.search(EXT_REGEX, fpath).group(1)


def get_prefix(fpath: str) -> str:
    """Extract prefix from path.

    :rtype: str
    :param fpath: the path to extract
    :return: the extracted prefix
    """
    ext_search = re.search(EXT_REGEX, fpath)
    return fpath[: ext_search.start()]


def get_path_format(wildcard: bool = False) -> str:
    """Get the format string of file paths.

    :rtype: str
    :param wildcard: whether it's a wildcard path or not.
    :return: the path format string
    """
    if wildcard:
        return DEFAULT_PATH_FORMAT + "/*"
    else:
        return DEFAULT_PATH_FORMAT + "/{filename}"
