"""Regular expression utilities."""
import re
from typing import Tuple


def find_all_groups(regex: str, s: str, grps: int) -> Tuple:
    """Find multiple groups in matching pattern.

    :param regex:
    :param s:
    :param grps:
    :return:
    """
    result = ()
    for match in re.finditer(regex, s):
        row = ()
        for i in range(1, grps + 1):
            row += (match.group(i),)
        result += (row,)
    return result
