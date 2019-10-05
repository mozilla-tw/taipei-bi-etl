"""Revenue task."""
from argparse import Namespace
from typing import Dict, Any, List, Tuple
import pandas as pd
import datetime
import pandasql as ps
from pandas import DataFrame
import utils.config
from tasks import base
import numpy as np
from utils.marshalling import get_country_tz_str, lookback_dates
import logging

log = logging.getLogger(__name__)

DEFAULTS = {}


class RevenueEtlTask(base.EtlTask):
    """ETL task to generate estimated revenue data for each market."""

    def __init__(
        self,
        args: Namespace,
        sources: Dict[str, Any],
        schema: List[Tuple[str, np.generic]],
        destinations: Dict[str, Any],
    ):
        """Initialize Revenue ETL task.

        :param args: args passed from command line,
        see `get_arg_parser()`
        :param sources: data source to be extracted,
        specified in task config, see `configs/*.py`
        :param schema: the target schema to load to.
        :param destinations: destinations to load data to,
        specified in task config, see `configs/*.py`
        """
        super().__init__(args, sources, schema, destinations, "staging", "revenue")

    def extract(self):
        """Inherit from super class and extract latest fb_index for later use."""
        super().extract()
        source = "bukalapak"
        if not self.args.source or source in self.args.source.split(","):
            config = self.sources[source]
            yesterday = lookback_dates(self.current_date, 1)
            if self.args.dest != "fs":
                self.extracted[source + "_base"] = self.extract_via_gcs(
                    source, config, "raw", yesterday
                )
            else:
                self.extracted[source + "_base"] = self.extract_via_fs(
                    source, config, "raw", yesterday
                )

    def transform_bukalapak(
        self, bukalapak: DataFrame, bukalapak_base: DataFrame, source: str
    ) -> DataFrame:
        """Transform data from bukalapak for revenue reference.

        Input: raw-revenue-bukalapak
        Output: staging-revenue-bukalapak

        :param bukalapak: extracted source DataFrame that contains transaction data
        :param bukalapak_base: extracted base DataFrame from yesterday for validation
        :rtype: DataFrame
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :return: the transformed DataFrame
        """
        # get revenue schema
        revenue_df = self.get_target_dataframe()

        # prepare data set
        # https://developers.tune.com/affiliate/affiliate_report-getconversions/
        map_cols = [
            "source",
            "Country.name",
            "ConversionsMobile.device_os",
            "Stat.datetime",
            "tz",
            "Stat.currency",
            "Stat.sale_amount",
            "Stat.approved_payout",
            "Stat.affiliate_info1",
            "Stat.affiliate_info2",
            "Stat.affiliate_info3",
            "Stat.affiliate_info4",
            "Stat.affiliate_info5",
            "Stat.conversion_status",
        ]

        # data pre-precessing: add common column
        def data_prep(d):
            log.info(">>> Start data preparation...")
            d["source"] = source
            d = d.replace("", np.nan)
            d["Country.name"] = [
                "ID" if x == "Indonesia" else "" for x in d["Country.name"]
            ]
            d["tz"] = d["Country.name"].apply(lambda x: get_country_tz_str(x))
            d = d[map_cols]
            log.info(">>> Done data preparation...")
            return d

        # checking functions
        # new df date range vs. args
        def check_dt_range():
            log.info(">>> Checking date range...")
            new_df_dt = pd.to_datetime(new_df["Stat.datetime"]).dt.date
            dt_start, dt_end = min(new_df_dt), max(new_df_dt)
            arg_start, arg_end = (
                self.last_month.date(),
                self.current_date.date() + datetime.timedelta(days=1),
            )
            assert (
                dt_end <= arg_end
            ), f">>> From {source}, Max(Date)={dt_end} greater then arg+1d={arg_end}."
            assert (
                dt_start >= arg_start
            ), f">>> From {source}, Min(Date)={dt_start} less then arg+1d={arg_start}."
            log.info(">>> Pass date range checking...")

        # schema match
        def check_schema():
            log.info(">>> Checking data schema matched...")
            match = list(set(map_cols) & set(new_df.columns))
            not_match = ", ".join(set(map_cols) - set(match))
            assert len(match) == len(
                map_cols
            ), f">>> Missing column [ {not_match} ] from {source}."
            log.info(">>> Pass data schema matched...")

        # invalid null value
        def check_null():
            log.info(">>> Checking invalid null value...")
            na_cols = new_df.columns[new_df.isna().any()].tolist()
            match = list(set(map_cols[0:8]) & set(na_cols))
            not_null = ", ".join(match)
            assert (
                len(match) == 0
            ), f">>> From {source}, values in column [ {not_null} ] should not be N/A."
            log.info(">>> Pass checking invalid null value...")

        # which to update (update & insert)
        def do_updates_inserts():
            new = new_df
            new["dt"] = self.current_date.date()
            old = last_df.copy()
            old["dt"] = self.current_date.date() - datetime.timedelta(days=1)
            comb = old.append(new)  # noqa: F841
            q = """
                SELECT source, max(`Stat.datetime`) as updated_key
                FROM comb
            """

            to_update = ps.sqldf(q)  # noqa: F841
            q = """
                SELECT comb.*
                FROM comb left join to_update
                on comb.source = to_update.source
                and comb.`Stat.datetime` = to_update.updated_key
            """
            do_update = ps.sqldf(q).drop(columns="dt")
            log.info(">>> Done updates and inserts...")
            return do_update

        # extract new & old data
        new_df = data_prep(bukalapak)
        last_df = bukalapak_base
        if not last_df.empty:
            last_df = data_prep(bukalapak_base)

            # transform here ------
        # do check
        for check in [check_dt_range, check_schema, check_null]:
            try:
                check()
            except AssertionError as e:
                raise e
        log.info(">>> Done data validation...")

        # do updates and inserts
        if last_df.empty:
            new_df.columns = revenue_df.columns
            df = revenue_df.append(new_df, ignore_index=True).drop_duplicates()
            log.info("init first batch")

        else:
            df = do_updates_inserts()
            df.columns = revenue_df.columns
            df = revenue_df.append(df, ignore_index=True).drop_duplicates()
            log.info("load new batch")

        df = df[df["conversion_status"] == "approved"]

        # reformat data types
        df["utc_datetime"] = df["utc_datetime"].astype("datetime64[ns]")
        df["tz"] = df["country"].apply(lambda x: get_country_tz_str(x))
        df["sales_amount"] = df["sales_amount"].astype("float")
        df["payout"] = df["payout"].astype("float")

        # fill N/A columns, so object columns w/t N/A won't be recognized as float64
        nacols = [
            "fx_defined1",
            "fx_defined2",
            "fx_defined3",
            "fx_defined4",
            "fx_defined5",
        ]
        for nacol in nacols:
            df[nacol] = df[nacol].fillna("")
        return df

    # def transform_google_search(
    #     self, google_search: DataFrame, google_search_rps: DataFrame
    # ) -> DataFrame:
    #     """Transform search data from telemetry for revenue reference.
    #
    #     Input: staging-rps-google_sesarch_rps, raw-revenue-google_search
    #     Output: staging-revenue-google_search
    #
    #     :rtype: DataFrame
    #     :param google_search: extracted source DataFrame for search volume
    #     :param google_search_rps: extracted source DataFrame for search rps
    #     :return: the transformed DataFrame
    #     """
    #     df = google_search
    #     df["country"] = df["country_code"]
    #     rps = google_search_rps
    #     df = df.join(rps, rsuffix="_rps")
    #     # transform here
    #     td = self.get_target_dataframe()
    #     td["os"] = df["os"]
    #     td["country"] = df["country_code"]
    #     # workaround for datetime64 validation since `datetime64[ns, UTC]`
    #     # will raise "TypeError: data type not understood"
    #     td["utc_datetime"] = df["day"].astype("datetime64[ns]")
    #     td["tz"] = df["country"].apply(lambda x: get_country_tz_str(x))
    #     td["payout"] = df["event_count"] * df["rps"]
    #     td["payout"] = td["payout"].fillna(0)
    #     td["sales_amount"] = td["sales_amount"].fillna(0)
    #     td["source"] = td["source"].fillna("google_search")
    #     td["currency"] = td["currency"].fillna("USD")
    #     # print(td)
    #     return td


def main(args: Namespace):
    """Take args and pass them to RevenueEtlTask.

    :param args: args passed from command line, see `base.get_arg_parser()`
    """
    config_name = ""
    if args.debug:
        config_name = "debug"
    if args.config:
        config_name = args.config
    configs = utils.config.get_configs("revenue", config_name)
    task = RevenueEtlTask(args, configs.SOURCES, configs.SCHEMA, configs.DESTINATIONS)
    log.info("Running Revenue Task.")
    task.run()
    log.info("Revenue Task Finished.")


if __name__ == "__main__":
    arg_parser = utils.config.get_arg_parser()
    main(arg_parser.parse_args())
