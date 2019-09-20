"""Mapping utilities."""
import importlib
import inspect
import itertools
from typing import Dict, Any, List, Callable

import pandas as pd
from pandas import DataFrame, Series

from configs import mapping
from utils.config import INJECTED_MAPPINGS
from utils.marshalling import flatten_dict, is_camel, decamelize


def map_apply(config: Dict[str, Any], df: DataFrame) -> DataFrame:
    """Apply map functions to extracted DataFrame.

    :param config: the source config to check mapping settings
    :param df: the extracted DataFrame to be applied
    """
    maps = import_map_funcs(config)
    # do nothing if no map functions found in config
    if not maps:
        return df
    # handle map product (e.g. feature x channel)
    map_list = []
    for map_name, map_types in maps.items():
        ls = []
        for t, map_funcs in map_types.items():
            ls += [(map_name, t, map_funcs)]
        map_list += [ls]
    map_prod = itertools.product(*map_list)
    output = DataFrame()
    # apply maps here
    for batch in map_prod:
        # use a clean DataFrame for each batch
        d = df.copy()
        for map_name, map_type, map_funcs in batch:
            for idx, row in d.iterrows():
                for map_func in map_funcs:
                    d = apply_map_func(d, idx, row, map_func, map_name, map_type)
        # TODO: dedup here
        output = output.append(d)
    output = output.reset_index()
    return output


def import_map_funcs(config: Dict[str, Any]) -> Dict[str, Dict[str, List[Callable]]]:
    """Import mapping functions according to source config.

    :rtype: Dict[str, Dict[str, List[Callable]]]
    :param config: the data source config
    :return: a dictionary containing all mapping functions
    """
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
            type_col = map_name + "_type"
            name_col = map_name + "_name"
            if type_col not in df.loc[idx].index:
                df[type_col] = Series(dtype=object)
            if name_col not in df.loc[idx].index:
                df[name_col] = Series(dtype=object)
            if "_" in map_type:
                # non-leaft node, ignore returns
                type_segments = map_type.split("_")
                t = type_segments[-1]
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
                    assert pd.isnull(df[type_col][idx]) or df[type_col][idx] == map_type
                    assert (
                        pd.isnull(df[name_col][idx]) or df[name_col][idx] == map_result
                    )
                    df[type_col][idx] = map_type
                    df[name_col][idx] = map_result
                elif isinstance(map_result, list):
                    df[type_col][idx] = map_type
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
