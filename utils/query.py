"""Query utilities."""
from typing import Dict, Any

from utils.file import read_string


def build_query(config: Dict[str, Any], start_date: str, end_date: str) -> str:
    """Build query based on configs and args.

    :rtype: str
    :param config: the config of the query
    :param start_date: the start date string for the query
    :param end_date: the end date string for the query
    :return: the composed query string
    """
    query = ""
    if "udf" in config:
        for udf in config["udf"]:
            query += read_string("udf/{}.sql".format(udf))
    if "udf_js" in config:
        for udf_js in config["udf_js"]:
            query += read_string("udf_js/{}.sql".format(udf_js))
    if "query" in config:
        query += read_string("sql/{}.sql".format(config["query"])).format(
            project=config["project"],
            dataset=config["dataset"],
            table=config["table"],
            start_date=start_date,
            end_date=end_date,
        )
    return query
