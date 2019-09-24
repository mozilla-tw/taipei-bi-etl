import re
from typing import Tuple


def find_all_groups(regex: str, s: str, grps: int) -> Tuple:
    result = ()
    for match in re.finditer(regex, s):
        row = ()
        for i in range(1, grps + 1):
            row += (match.group(i), )
        result += (row, )
    return result
