"""Mapping utilities."""
import importlib
import inspect
import logging
from typing import Dict, Any, List, Callable
import pandas as pd
from pandas import DataFrame, Series
from configs import mapping
from utils.config import INJECTED_MAPPINGS
from utils.marshalling import flatten_dict, is_camel, decamelize


log = logging.getLogger(__name__)

MAP_TYPE = "map_type"
MAP_NAME = "map_name"


def map_apply(config: Dict[str, Any], df: DataFrame) -> DataFrame:
    """Apply map functions to extracted DataFrame.

    :param config: the source config to check mapping settings
    :param df: the extracted DataFrame to be applied
    """
    maps = import_map_funcs(config)
    # do nothing if no map functions found in config
    if not maps:
        return df
    # prepare map function batches (e.g. feature x channel)
    map_list = []
    for map_name, map_types in maps.items():
        for t, map_funcs in map_types.items():
            map_list += [(map_name, t, map_funcs)]
    output = DataFrame()
    # apply maps here
    for map_type, map_name, map_funcs in map_list:
        # use a clean DataFrame for each batch
        d = df.copy()
        for idx, row in d.iterrows():
            for map_func in map_funcs:
                d = apply_map_func(d, idx, row, map_func, map_name, map_type)
        # dedup events here
        dedup = dedup_mapped_rows(d)
        output = output.append(dedup)
    output = output.reset_index()
    return output


def dedup_mapped_rows(d: DataFrame) -> DataFrame:
    """Deduplicate mapped rows.

    :param d: the DataFrame to be de-duplicated
    :return: the de-duplicated DataFrame
    """
    map_cols = [MAP_TYPE, MAP_NAME]
    dedup = DataFrame()
    for id in d["id"].unique():
        unnested_event = d[d["id"] == id]
        cols = {}
        for map_col in map_cols:
            col = unnested_event[map_col]
            col = col[pd.notnull(col)].tolist()
            if len(col) > 0:
                if isinstance(col[0], str):
                    # check consistency for string values
                    col = list(set(col))
                    if len(col) == 1:
                        cols[map_col] = col[0]
                    else:
                        assert False, "inconsistent mapping %s for id %s" % (
                            str(col),
                            id,
                        )
                elif isinstance(col[0], list):
                    # merge list values
                    cols[map_col] = list(set([i for l in col for i in l]))
                else:
                    assert False, "invalid mapping data type %s: %s" % (
                        str(col[0]),
                        str(type(col[0])),
                    )
        # only keep rows with mapped values
        if cols:
            reduced_event = unnested_event.head(1).copy()
            for k, v in cols.items():
                reduced_event[k] = [v]
            dedup = dedup.append(reduced_event)
        else:
            log.debug("id %s skipped" % id)
    return dedup


def import_map_funcs(config: Dict[str, Any]) -> Dict[str, Dict[str, List[Callable]]]:
    """Import mapping functions according to source config.

    :rtype: Dict[str, Dict[str, List[Callable]]]
    :param config: the data source config
    :return: a dictionary containing all mapping functions
    """
    # FIXME: dedup map funcs here
    maps = {}
    if "mappings" in config:
        for m in config["mappings"]:
            maps[m] = {}
            # import mapping module from config
            mod = importlib.import_module("%s.%s" % (mapping.__name__, m))
            if m in INJECTED_MAPPINGS:
                mod.MAPPING = INJECTED_MAPPINGS[m]
            # iterate through map type classes (Feature, Vertical, App, ...)
            # {mod: {cls: subcls: func: map_func}}
            maptree = extract_map_funcs_recursive(mod)
            # {mod: {type: [map_func, ...]}}
            # type = cls_type, subcls_type, func_type
            mapflat = flatten_dict(maptree)
            for types, map_func in mapflat.items():
                ts = []
                for name in types.split(","):
                    if is_camel(name):
                        # non-leaf node
                        ts += [decamelize(name)]
                    else:
                        # leaf node
                        ts += [name.split("_")[-1]]
                for t in ts:
                    if t not in maps[m]:
                        maps[m][t] = []
                    if map_func not in maps[m][t]:
                        maps[m][t] += [map_func]
    return maps


def extract_map_funcs_recursive(mod: Callable, clsobj: Callable = None) -> Dict:
    """Extract map functions based on mapping config hierarchy.

    :param mod: the package of mapping configs
    :param clsobj: the module of the mapping config
    :return: extracted map functions in nested dictionary
    """
    maps = {}
    for cls, cobj in inspect.getmembers(
        mod if not clsobj else clsobj, predicate=inspect.isclass
    ):
        # filter only classes defined in the mapping module,
        # exclude imported or other builtin classes
        if hasattr(cobj, "__module__") and cobj.__module__ == mod.__name__:
            # iterate through static functions in the class
            for method, mobj in inspect.getmembers(cobj, predicate=inspect.isroutine):
                # verify the method is defined directly on the class,
                # not inherited or builtin methods,
                # also verify it's a static method.
                if method in cobj.__dict__ and isinstance(
                    cobj.__dict__[method], staticmethod
                ):
                    if cls not in maps:
                        maps[cls] = {}
                    maps[cls][method] = mobj
            submaps = extract_map_funcs_recursive(mod, cobj)
            # merge dictionaries
            maps[cls] = {
                **(maps[cls] if cls in maps else {}),
                **(submaps if submaps else {}),
            }
    return maps


def apply_map_func(
    df: DataFrame,
    idx: int,
    row: Series,
    map_func: Callable,
    map_name: str,
    map_type: str,
) -> DataFrame:
    """Apply map functions to a row of DataFrame.

    :param df: the DataFrame to apply
    :param idx: the row to apply
    :param row: the actual row (Series)
    :param map_func: the map function
    :param map_name: the name of the mapping
    :param map_type: the type of the mapping
    """
    with pd.option_context("mode.chained_assignment", None):
        map_result = map_func(row)
        if map_result:
            type_col = MAP_TYPE
            name_col = MAP_NAME
            if type_col not in df.loc[idx].index:
                df[type_col] = Series(dtype=object)
            if name_col not in df.loc[idx].index:
                df[name_col] = Series(dtype=object)
            if "_" in map_type:
                # non-leaft node, ignore returns
                type_segments = map_type.split("_")
                t = type_segments[-1] + map_name
                n = "_".join(type_segments[0:-1])
                if idx in df[type_col] and pd.notnull(df[type_col][idx]):
                    assert df[type_col][idx] == t, "%s != %s" % (df[type_col][idx], t)
                if idx in df[name_col] and pd.notnull(df[name_col][idx]):
                    assert df[name_col][idx] == n, "%s != %s" % (df[name_col][idx], n)
                df[type_col][idx] = t
                df[name_col][idx] = n
            else:
                # leaf node, use return value as name
                if isinstance(map_result, str):
                    # Check duplicated mapping
                    assert pd.isnull(df[type_col][idx])
                    assert pd.isnull(df[name_col][idx])
                    df[type_col][idx] = map_type + "_" + map_name
                    df[name_col][idx] = map_result
                elif isinstance(map_result, list):
                    df[type_col][idx] = map_type + "_" + map_name
                    # merge list if duplicated
                    if isinstance(df[name_col][idx], list):
                        df[name_col][idx] += map_result
                    elif pd.isnull(df[name_col][idx]):
                        df[name_col][idx] = map_result
                    else:
                        assert False, "Invalid data type found %s: %s" % (
                            map_type,
                            str(type(df[name_col][idx])),
                        )
                else:
                    assert False, "Invalid mapping result %s: %s" % (
                        map_type,
                        str(map_result),
                    )
    return df
