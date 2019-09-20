"""Marshalling utilities."""
import datetime
import json
import re
from io import StringIO
from collections import Counter
from functools import reduce
from typing import Optional, Dict, List, Any
import pandas.io.json as pd_json
import pandas as pd
import pytz
import logging

from pandas import DataFrame, Series

from utils.config import DEFAULT_TZ_FORMAT, DEFAULT_DATETIME_FORMAT

log = logging.getLogger(__name__)


def convert_df(raw: str, config: Dict[str, Any]) -> DataFrame:
    """Convert raw string to DataFrame, currently only supports json/csv.

    :rtype: DataFrame
    :param raw: the raw source string in json/csv format,
        this is to be converted to DataFrame
    :param config: the config of the data source specified in task config,
        see `configs/*.py`
    :return: the converted `DataFrame`
    """
    ftype = "json" if "file_format" not in config else config["file_format"]
    df = None
    if ftype == "jsonl":
        jlines = raw.split("\n")
        df = DataFrame()
        for jline in jlines:
            if len(jline) < 3:
                continue
            line = json.loads(jline)
            df = df.append(Series(line), ignore_index=True)
    elif ftype == "json":
        if "json_path" in config:
            extracted_json = json_extract(raw, config["json_path"])
        elif "json_path_nested" in config:
            extracted_json = json_unnest(
                raw, config["json_path_nested"], config["fields"], {}, []
            )
        else:
            extracted_json = raw
        data = pd_json.loads(extracted_json)
        df = pd_json.json_normalize(data)
    elif ftype == "csv":
        if "header" in config:
            df = pd.read_csv(StringIO(raw), names=config["header"])
        else:
            df = pd.read_csv(StringIO(raw))
    # convert timezone according to config
    tz = None
    if "timezone" in config:
        tz = pytz.timezone(config["timezone"])
    elif "country_code" in config:
        tz = get_country_tz(config["country_code"])
    # TODO: support multiple countries/timezones in the future if needed
    if "date_fields" in config:
        for date_field in config["date_fields"]:
            df[date_field] = pd.to_datetime(df[date_field])
        if tz is not None:
            df["tz"] = get_tz_str(tz)
            for date_field in config["date_fields"]:
                df[date_field] = (
                    df[date_field].dt.tz_localize(tz).dt.tz_convert(pytz.utc)
                )
                df[date_field] = df[date_field].astype("datetime64[ns]")
    return df


def convert_format(format: str, df: DataFrame, date_fields: List = None) -> str:
    """Convert DataFrame into destination format.

    The logic is based on task config (see `configs/*.py`).

    :param format: the format to convert
    :param df: The DataFrame to be converted to destination format.
    :param date_fields: the date fields to convert to date string
    :return: the output string in converted format
    """
    output = ""
    if date_fields:
        for date_field in date_fields:
            df[date_field] = df[date_field].dt.strftime(DEFAULT_DATETIME_FORMAT)
    if format == "jsonl":
        output = ""
        # build json lines
        for row in df.iterrows():
            output += row[1].to_json() + "\n"
    elif format == "json":
        output = "["
        # build json
        for row in df.iterrows():
            output += row[1].to_json() + ",\n"
        if len(output) > 2:
            output = output[0:-2] + "\n]"
    elif format == "csv":
        output = df.to_csv(index=False)
    return output


def json_extract(json_str: str, path: str) -> Optional[str]:
    """Extract nested json element by path.

    Note that this currently don't support nested json array in path.

    :rtype: str
    :param json_str: original json in string format
    :param path: path of the element in string format, e.g. response.data
    :return: the extracted json element in string format

    >>> j = json.dumps({"level1":{"level2": {"level3":"extract me"}}})
    >>> json_extract(j, "level1.level2.level3")
    'extract me'
    """
    j = json.loads(json_str)
    if not path:
        return json.dumps(j)
    if isinstance(path, str):
        for i in path.split("."):
            if i in j:
                j = j[i]
            else:
                return None
    if isinstance(j, list) or isinstance(j, dict):
        return json.dumps(j)
    else:
        return j


def json_unnest(
    json_str: str,
    paths: List[str],
    fields: List[str],
    ancestors: Dict,
    result: List,
    level: int = 0,
) -> Optional[str]:
    """Flatten nested json elements of specified paths.

    :param json_str: original json in string format
    :param paths: paths of the nested elements in string format
    :param fields: fields to extract
    :param ancestors: elements from ancestors
    :param result: the recursively accumulated result
    :param level: current recursive level
    :return: the extracted json element in string format
    :rtype: str
    """
    if not paths:
        return json_str
    ancestors = {} if not ancestors else ancestors
    if isinstance(paths, list):
        # flatten nested json here
        path = paths[level]
        path_leaf = singularize(path.split(".")[-1]).lower()
        extracted = json_extract(json_str, path)
        if not extracted:
            return None
        # expected to be json array
        elems = json.loads(extracted)
        for elem in elems:
            vals = ancestors.copy()
            for field in fields:
                vals[path_leaf + "_" + field.lower()] = elem[field]
            if level + 1 < len(paths):
                r = json_unnest(
                    json.dumps(elem), paths, fields, vals, result, level + 1
                )
                if r is None:
                    result += [vals]
            else:
                result += [vals]

    else:
        assert False, "paths should be a list of strings"
    return json.dumps(result)


def singularize(s: str) -> str:
    """Make plural name singular.

    :param s: the string to be singularized
    :return: the singularized string

    >>> singularize("Networks")
    'Network'
    """
    if s[-1] == "s":
        s = s[0:-1]
    return s


def get_country_tz(country_code: str) -> pytz.UTC:
    """Get the default timezone for specified country code.

    If covered multiple timezone, pick the most common one.

    :rtype: pytz.UTC
    :return: the default (major) timezone of the country
    :param country_code: the 2 digit country code to get timezone

    >>> get_country_tz("TW")
    <DstTzInfo 'Asia/Taipei' LMT+8:06:00 STD>
    """
    if not country_code:
        log.warning("No country code specified, returning UTC.")
        return pytz.UTC
    country_code = country_code.upper()
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

    >>> get_country_tz_str("TW")
    '+08:00'
    """
    return get_tz_str(get_country_tz(country_code))


def get_tz_str(timezone: pytz.UTC) -> str:
    """Convert timezone to offset string (e.g. +08:00).

    :rtype: str
    :param timezone: pytz.UTC
    :return: the timezone offset string in +08:00 format.

    >>> get_tz_str(pytz.UTC)
    '+00:00'
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

    >>> lookback_dates(datetime.datetime(2019, 1, 1), 30)
    datetime.datetime(2018, 12, 2, 0, 0)
    """
    return date - datetime.timedelta(days=period)


def lookfoward_dates(date: datetime.datetime, period: int) -> datetime.datetime:
    """Add date by period.

    :rtype: datetime.datetime
    :param date: the base date
    :param period: the period to add
    :return: the add datetime

    >>> lookfoward_dates(datetime.datetime(2019, 1, 1), 30)
    datetime.datetime(2019, 1, 31, 0, 0)
    """
    return date + datetime.timedelta(days=period)


def flatten_dict(d: Dict, pref: str = "", separator=",") -> Dict:
    """Flatten nested dictionary.

    :param d: the dictionary to flatten
    :param pref: the prefix of the dictionary keys
    :param separator: the level separator string, default to ","
    :return: the flatten dictionary

    >>> flatten_dict({"level1": {"level2": {"level3-1": "a", "level3-2": "b"}}})
    {'level1,level2,level3-1': 'a', 'level1,level2,level3-2': 'b'}
    """
    pref = pref if pref == "" else pref + separator
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

    >>> is_camel("ThisIsCamel")
    True
    >>> is_camel("this_is_not_camel")
    False
    """
    return name != name.lower() and name != name.upper() and "_" not in name


def decamelize(name: str) -> str:
    """Conver camel case name into lower case + underscored name.

    :param name: the name to decamelize
    :return: the de-camelized name

    >>> decamelize("DecamelizeMe")
    'decamelize_me'
    """
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
