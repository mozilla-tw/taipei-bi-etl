"""Common."""
from pandas import DataFrame
from utils.marshalling import convert_df


def cachedDataFrame(fpath, config) -> DataFrame:
    """Open dataframe stored in file."""
    with open(fpath, "r") as f:
        raw = f.read()
        return convert_df(raw, config)
