"""Data anonymizer utility."""
import csv
import glob
import json
import random
import re
import string
from typing import Dict, List, Tuple, Any, Union

EXT_REGEX = "[*0-9A-z]+\\.([A-z0-9]+$)"
DATA_PATH = "test-data"
ANONYMIZE_CONFIG = {
    "raw-revenue-bukalapak": {
        "response.data.data": [
            ("Stat.ip", "ip"),
            ("Stat.session_ip", "ip"),
            ("Stat.sale_amount", float),
            ("Stat.approved_payout", float),
            ("Stat.sale_amount@IDR", float),
            ("Stat.approved_payout@IDR", float),
            ("Stat.ad_id", "uid"),
        ]
    },
    "raw-revenue-google_search": [("event_count", int)],
    "raw-rps-google_search_rps": [("volume", int)],
    "raw-rps-fb_index": [("cost_index", float)],
    "raw-rps-cb_index": [(4, float)],
    # "staging-revenue-bukalapak": [("sales_amount", float), ("payout", float)],
    # "staging-revenue-google_search": [("sales_amount", float), ("payout", float)],
    # "staging-rps-google_search_rps": [("rps", float)],
}


def main():
    """Run anonymizer."""
    for name, cfgs in ANONYMIZE_CONFIG.items():
        files = glob.glob("./%s/%s/*" % (DATA_PATH, name))
        for file in files:
            ext = re.search(EXT_REGEX, file).group(1)
            d = read_data(cfgs, ext, file)
            write_data(d, ext, file)


def read_data(cfgs: Union[Dict, List], ext: str, file: str) -> object:
    """Read and anonymize data.

    :param cfg: the anonymize config
    :param d: the data to be anonymized
    :param ext: the extension of the data file
    :param file: the file name
    :return: the anonymized data
    """
    d = None
    with open(file, "r") as f:
        if ext == "jsonl":
            d = []
            for l in f:
                d += [json.loads(l)]
        elif ext == "json":
            d = json.load(f)
        elif ext == "csv":
            has_header = csv.Sniffer().has_header(f.read(2048))
            f.seek(0)
            if has_header:
                r = csv.DictReader(f)
            else:
                r = csv.reader(f)
            d = []
            for row in r:
                d += [row]
        anonymize_data(cfgs, d)
    return d


def write_data(d: Union[List, Dict], ext: str, file: str):
    """Write anonymized data.

    :param d: the data to be anonymized
    :param ext: the extension of the data file
    :param file: the file name
    """
    with open(file, "w") as f:
        if ext == "jsonl":
            o = ""
            for row in d:
                o += json.dumps(row) + "\n"
            f.write(o)
        elif ext == "json":
            o = json.dumps(d)
            f.write(o)
        elif ext == "csv":
            if isinstance(d[0], dict):
                dr = csv.DictWriter(f, fieldnames=d[0].keys())
                dr.writeheader()
                dr.writerows(d)
            elif isinstance(d[0], list):
                dr = csv.writer(f)
                dr.writerows(d)


def anonymize_data(cfgs: Union[Dict, List], d: Union[Dict, List]):
    """Anonymize data based on config.

    :param cfg: the anonymize config
    :param d: the data to be anonymized
    """
    if isinstance(cfgs, list):
        for row in d:
            for cfg in cfgs:
                anonymize_row(cfg, row)
    elif isinstance(cfgs, dict):
        for path, cs in cfgs.items():
            ed = extract(d, path)
            for row in ed:
                for c in cs:
                    anonymize_row(c, row)


def anonymize_row(cfg: Tuple, row: Dict):
    """Anonymize row based on config.

    :param cfg: the anonymize config
    :param row: the row to be anonymized
    """
    anonymous_data = None
    if cfg[1] == "ip":
        anonymous_data = "%d.%d.%d.%d" % (
            random.random() * 255,
            random.random() * 255,
            random.random() * 255,
            random.random() * 255,
        )
    elif cfg[1] == "uuid":
        anonymous_data = "".join(
            random.choices(string.ascii_uppercase + string.digits, k=20)
        )
    elif cfg[1] == float:
        anonymous_data = random.random() * random.random() * 1000
    elif cfg[1] == int:
        anonymous_data = int(random.random() * random.random() * 1000)
    extract(row, cfg[0], anonymous_data)


def extract(data: Dict, path: Union[str, int], newval=None) -> Any:
    """Extract nested json element by path.

    Note that this currently don't support nested json array in path.

    :rtype: str
    :param data: the data to extract
    :param path: path of the element in string format, e.g. response.data
    :param newval: the new value to assign
    :return: the extracted json element in string format
    """
    if path:
        parent = data
        idx = path
        if isinstance(path, str):
            for i in path.split("."):
                if i in data:
                    idx = i
                    parent = data
                    data = data[i]
                else:
                    return None
        if newval is not None:
            parent[idx] = newval
    return data


if __name__ == "__main__":
    main()
