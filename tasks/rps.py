"""RPS task."""
import datetime
from argparse import Namespace
from typing import Dict, Any, List, Tuple

from pandas import DataFrame
from tasks import base
import pycountry
import pandas as pd
import numpy as np
import logging

log = logging.getLogger(__name__)

DEFAULTS = {"date": datetime.datetime(2018, 1, 1), "period": 365}


class RpsEtlTask(base.EtlTask):
    """ETL task to generate estimated revenue per search data for each market."""

    def __init__(
        self,
        args: Namespace,
        sources: Dict[str, Any],
        schema: List[Tuple[str, np.generic]],
        destinations: Dict[str, Any],
    ):
        """Initialize RPS ETL task.

        :param args: args passed from command line,
        see `get_arg_parser()`
        :param sources: data source to be extracted,
        specified in task config, see `configs/*.py`
        :param schema: the target schema to load to.
        :param destinations: destinations to load data to,
        specified in task config, see `configs/*.py`
        """
        super().__init__(args, sources, schema, destinations, "staging", "rps")
        self.extracted_idx = dict()

    def extract(self):
        """Inherit from super class and extract latest fb_index for later use."""
        super().extract()
        source = "fb_index"
        if not self.args.source or source in self.args.source.split(","):
            config = self.sources[source]
            self.extracted_idx[source] = self.extract_via_api_or_cache(
                source,
                config,
                "raw",
                RpsEtlTask.lookfoward_dates(self.current_date, self.period),
            )[0]

    def transform_google_search_rps(
        self, source: str, config: Dict[str, Any]
    ) -> DataFrame:
        """Calculate revenue per search with existing CPI index and total package.

        Country RPS = Country CPI Index * Revenue Share Factor
            (Assume the same for all Countries)
        Revenue Share Factor = Country RPS / Country CPI Index
            = (Country Revenue / Country Searches) / Country CPI Index
            = (
                (
                    Total Revenue * Country Searches * Country CPI Index
                    / Σ(Country Searches * Country CPI Index
                )
            )
            / Country Searches) / Country CPI Index
            = Total Revenue / Σ(Country Searches * Country CPI Index)
        :rtype: DataFrame
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :return: the transformed DataFrame
        """
        # shared functions to map/transform data
        def map_country_3_to_2(alpha_3):
            c = pycountry.countries.get(alpha_3=alpha_3)
            if c:
                return c.alpha_2
            return None

        def transform_fb_idx(idx):
            assert len(idx.index) > 200, "Too few rows in FB index"
            country_2 = idx["country_code"].apply(map_country_3_to_2)
            idx["country"] = country_2
            return idx.drop_duplicates("country").set_index("country")

        def avg_idx(idxmap, col):
            cost_idx = None
            for i, idx in idxmap.items():
                cidx = transform_fb_idx(idx)
                if None is cost_idx:
                    cost_idx = cidx[col].astype(float)
                else:
                    cost_idx += cidx[col].astype(float)
            cost_idx /= len(idxmap)
            return cost_idx

        def transform_cb_idx(idx):
            # 2017 mobile market share
            android_share = 72.63
            ios_share = 19.65
            mobile_base = android_share + ios_share
            android_idx = idx.copy()[
                (idx["platform"] == "Google Play")
                & (idx["category"] == "Average")
                & (idx["cpi"] > 0)
            ]
            android_idx["cpi"] = android_idx["cpi"] / mobile_base * android_share
            android_idx["country"] = android_idx["country_code"]
            android_idx = android_idx.set_index("country")
            ios_idx = idx.copy()[
                (idx["platform"] == "iOS")
                & (idx["category"] == "Average")
                & (idx["cpi"] > 0)
            ]
            ios_idx["cpi"] = ios_idx["cpi"] / mobile_base * ios_share
            ios_idx["country"] = ios_idx["country_code"]
            ios_idx = ios_idx.set_index("country")
            android_idx["cpi"] += ios_idx["cpi"]
            android_idx = android_idx[android_idx["cpi"] > 0]
            return android_idx

        def get_rps_factor(volume, cost_idx, package):
            s = volume * cost_idx
            return package / s.sum()

        df = self.extracted[source]
        pkg = self.extracted["global_package"]
        # use FB cost index as revenue index since it covers all os/categories/trends
        cost_idx_base = avg_idx(self.extracted["fb_index"], "cost_index")
        cost_idx_latest = avg_idx(self.extracted_idx["fb_index"], "cost_index")
        # CB as reference only since it"s only for mobile game.
        cost_idx_cb = transform_cb_idx(self.extracted["cb_index"])

        df = pd.pivot_table(df, index="country", values="volume", aggfunc=np.sum)

        df["cost_idx_base"] = cost_idx_base
        df["cost_idx_latest"] = cost_idx_latest
        df["cost_idx_cb"] = cost_idx_cb["cpi"]
        df = df.reset_index()
        df = df[
            df["country"].str.match("^[0-9A-Z]{2}$")
            & (df["cost_idx_base"] > 0)
            & (df["volume"] > 0)
        ]
        rps_factor = get_rps_factor(
            df["volume"], df["cost_idx_base"], pkg["package"][0]
        )
        log.info("Facebook RPS Factor: %d" % rps_factor)
        cb_rps_factor = get_rps_factor(
            df["volume"], df["cost_idx_cb"], pkg["package"][0]
        )
        log.info("Chartboost RPS Factor: %d" % cb_rps_factor)
        df["rps"] = df["cost_idx_latest"] * rps_factor
        df["rps_cb"] = df["cost_idx_cb"] * cb_rps_factor
        df["cb_rps_ratio"] = df["rps_cb"] / df["rps"]
        assert len(df.index) > 200, "Too few rows in transformed data"
        for col in ["country", "volume", "rps", "cost_idx_latest"]:
            assert not df[col].isnull().values.any(), "Null value in %s" % col
        # print(df)
        # print(df[df["country"].isin(["IN", "ID", "TW", "HK", "SG", "US", "DE"])])
        return df


def main(args: Namespace):
    """Take args and pass them to RpsEtlTask.

    :param args: args passed from command line, see `base.get_arg_parser()`
    """
    config_name = ""
    if args.debug:
        config_name = "debug"
    if args.config:
        config_name = args.config
    configs = base.get_configs("rps", config_name)
    task = RpsEtlTask(args, configs.SOURCES, configs.SCHEMA, configs.DESTINATIONS)
    task.run()


if __name__ == "__main__":
    arg_parser = base.get_arg_parser(**DEFAULTS)
    main(arg_parser.parse_args())
