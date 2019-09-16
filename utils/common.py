from pandas import DataFrame
from tasks.base import EtlTask


def cachedDataFrame(fpath, config) -> DataFrame:
    with open(fpath, "r") as f:
        raw = f.read()
        return EtlTask.convert_df(raw, config)
