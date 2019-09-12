"""Marshalling utilities."""
import datetime
import json
import re
from collections import Counter
from functools import reduce
from typing import Optional, Dict
import pytz
import logging
from utils.config import DEFAULT_TZ_FORMAT

log = logging.getLogger(__name__)


def json_extract(json_str: str, path: str) -> Optional[str]:
    """Extract nested json element by path.

    Note that this currently don't support nested json array in path.

    :rtype: str
    :param json_str: original json in string format
    :param path: path of the element in string format, e.g. response.data
    :return: the extracted json element in string format
    """
    j = json.loads(json_str)
    if path:
        for i in path.split("."):
            if i in j:
                j = j[i]
            else:
                return None
    return json.dumps(j)


def get_country_tz(country_code: str) -> pytz.UTC:
    """Get the default timezone for specified country code.

    If covered multiple timezone, pick the most common one.

    :rtype: pytz.UTC
    :return: the default (major) timezone of the country
    :param country_code: the 2 digit country code to get timezone
    """
    if country_code not in pytz.country_timezones:
        # FIXME: workaround here for pytz doesn't support XK for now.
        tzmap = {"XK": "CET"}
        if country_code in tzmap:
            return pytz.timezone(tzmap[country_code])
        log.warning("timezone not found for %s, return UTC" % country_code)
        return pytz.utc
    timezones = pytz.country_timezones[country_code]
    offsets = []
    for timezone in timezones:
        try:
            offsets += [
                pytz.timezone(timezone).utcoffset(datetime.datetime.now()).seconds
                / 3600
            ]
        except pytz.exceptions.NonExistentTimeError:
            log.warning("Error creating timezone")
            log.warning(timezones)
    if not offsets:
        log.warning("returning UTC")
        return pytz.UTC
    offset_count = Counter(offsets)
    max_count = -1
    max_offset = None
    for k, v in offset_count.items():
        if v > max_count:
            max_count = v
            max_offset = k
    return pytz.timezone(timezones[offsets.index(max_offset)])


def get_country_tz_str(country_code: str) -> str:
    """Get the default timezone string (e.g. +08:00) for specified country code.

    If covered multiple timezone, pick the most common one.

    :rtype: str
    :param country_code: the 2 digit country code to get timezone
    :return: the default (major) timezone string of the country in +08:00 format.
    """
    return get_tz_str(get_country_tz(country_code))


def get_tz_str(timezone: pytz.UTC) -> str:
    """Convert timezone to offset string (e.g. +08:00).

    :rtype: str
    :param timezone: pytz.UTC
    :return: the timezone offset string in +08:00 format.
    """
    return DEFAULT_TZ_FORMAT % (
        timezone.utcoffset(datetime.datetime.now()).seconds / 3600,
    )


def lookback_dates(date: datetime.datetime, period: int) -> datetime.datetime:
    """Subtract date by period.

    :rtype: datetime.datetime
    :param date: the base date
    :param period: the period to subtract
    :return: the subtracted datetime
    """
    return date - datetime.timedelta(days=period)


def lookfoward_dates(date: datetime.datetime, period: int) -> datetime.datetime:
    """Add date by period.

    :rtype: datetime.datetime
    :param date: the base date
    :param period: the period to add
    :return: the add datetime
    """
    return date + datetime.timedelta(days=period)


def flatten_dict(d: Dict, pref: str = "") -> Dict:
    """Flatten nested dictionary.

    :param d: the dictionary to flatten
    :param pref: the prefix of the dictionary keys
    :return: the flatten dictionary
    """
    pref = pref if pref == "" else pref + ","
    return reduce(
        lambda new_d, kv: (
            isinstance(kv[1], dict)
            and {**new_d, **flatten_dict(kv[1], pref + kv[0])}
            or {**new_d, pref + kv[0]: kv[1]}
        ),
        d.items(),
        {},
    )


def is_camel(name: str) -> bool:
    """Check if a name is camel-cased.

    :param name: the name to check
    :return: whether the name is camel-cased
    """
    return name != name.lower() and name != name.upper() and "_" not in name


def decamelize(name: str) -> str:
    """Conver camel case name into lower case + underscored name.

    :param name: the name to decamelize
    :return: the de-camelized name
    """
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
