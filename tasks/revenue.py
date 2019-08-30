import pandas as pd
import datetime
import pandasql as ps
from tasks import base
from configs import revenue
from configs.debug import revenue as revenue_dbg
import numpy as np

DEFAULTS = {}


class RevenueEtlTask(base.EtlTask):

    def __init__(self, args, sources, schema, destinations):
        super().__init__(args, sources, schema, destinations, 'staging', 'revenue')

    def transform_bukalapak(self, source, config):
        """ Transform data from bukalapak into unified format for revenue reference

        :rtype: DataFrame
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :return: the transformed DataFrame
        """

        # get revenue schema
        revenue_df = self.get_target_dataframe()

        # prepare data set
        # https://developers.tune.com/affiliate/affiliate_report-getconversions/
        map_cols = [
            'source',
            'Country.name',
            'ConversionsMobile.device_os',
            'Stat.datetime',
            'tz',
            'Stat.currency',
            'Stat.sale_amount',
            'Stat.approved_payout',
            'Stat.affiliate_info1',
            'Stat.affiliate_info2',
            'Stat.affiliate_info3',
            'Stat.affiliate_info4',
            'Stat.affiliate_info5',
            'Stat.conversion_status'
        ]

        # extract new data
        new_df = self.extracted[source]
        new_df = new_df.replace('', np.nan)
        new_df['Country.name'] = ['ID' if x == 'Indonesia' else '' for x in
                                  new_df['Country.name']]
        new_df['source'] = source
        new_df['tz'] = new_df['Country.name'].apply(
            lambda x: RevenueEtlTask.get_country_tz_str(x))

        new_df = new_df[map_cols]

        # extract old data
        last_df = self.extracted_base[source]
        if not last_df.empty:
            last_df['source'] = source
            last_df['Country.name'] = [
                'ID' if x == 'Indonesia' else '' for x in last_df['Country.name']]
            last_df['tz'] = last_df['Country.name'].apply(
                lambda x: RevenueEtlTask.get_country_tz_str(x))
            last_df = last_df[map_cols]

        # checking functions
        # new df date range vs. args
        def check_dt_range():
            print('>>> Checking date range...')
            new_df_dt = pd.to_datetime(new_df['Stat.datetime']).dt.date
            dt_start, dt_end = min(new_df_dt), max(new_df_dt)
            arg_start, arg_end = \
                self.last_month.date(), \
                self.current_date.date() + datetime.timedelta(days=1)
            print(dt_start, dt_end)
            print(arg_start, arg_end)
            assert dt_end <= arg_end, \
                f'>>> From {source}, Max(Date)={dt_end} greater then arg+1d={arg_end}.'
            assert dt_start >= arg_start, \
                f'>>> From {source}, Min(Date)={dt_start} less then arg+1d={arg_start}.'
            print('>>> Pass date range checking...')

        # schema match
        def check_schema():
            print('>>> Checking data schema matched...')
            match = list(set(map_cols) & set(new_df.columns))
            not_match = ', '.join(set(map_cols) - set(match))
            assert len(match) == len(
                map_cols), f'>>> Missing column [ {not_match} ] from {source}.'
            print('>>> Pass data schema matched...')

        # invalid null value
        def check_null():
            print('>>> Checking invalid null value...')
            na_cols = new_df.columns[new_df.isna().any()].tolist()
            match = list(set(map_cols[0:7]) & set(na_cols))
            not_null = ', '.join(match)
            assert len(match) == 0, \
                f'>>> From {source}, values in column [ {not_null} ] should not be N/A.'
            print('>>> Pass checking invalid null value...')

        # which to update (update & insert)
        def do_updates_inserts():
            new = new_df
            new['dt'] = self.current_date.date()
            old = last_df.copy()
            old['dt'] = self.current_date.date() - datetime.timedelta(days=1)
            comb = old.append(new)
            q = """
                SELECT source, max(`Stat.datetime`) as updated_key
                FROM comb
            """

            to_update = ps.sqldf(q)
            q = """
                SELECT comb.*
                FROM comb left join to_update
                on comb.source = to_update.source
                and comb.`Stat.datetime` = to_update.updated_key
            """
            do_update = ps.sqldf(q).drop(columns='dt')
            print('>>> Done updates and inserts...')
            return do_update

        # transform here ------
        # do check
        for check in [check_dt_range, check_schema, check_null]:
            try:
                check()
            except AssertionError as e:
                raise e
        print('>>> Done data validation...')

        # do updates and inserts
        if last_df.empty:
            new_df.columns = revenue_df.columns
            df = revenue_df.append(new_df, ignore_index=True)
            print('init first batch')

        else:
            df = do_updates_inserts()
            df.columns = revenue_df.columns
            df = revenue_df.append(df, ignore_index=True)
            print('load new batch')

        df = df[df['conversion_status'] == 'approved']

        # reformat data types
        df['utc_datetime'] = df['utc_datetime'].astype('datetime64[ns]')
        df['tz'] = df['country'].apply(lambda x: RevenueEtlTask.get_country_tz_str(x))
        df['sales_amount'] = df['sales_amount'].astype('float')
        df['payout'] = df['payout'].astype('float')
        print(df.head(4).to_string())
        return df

    def transform_google_search(self, source, config):
        """ Transform search data from telemetry into unified format
        for revenue reference

        :rtype: DataFrame
        :param source: name of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :param config: config of the data source to be extracted,
            specified in task config, see `configs/*.py`
        :return: the transformed DataFrame
        """

        df = self.extracted[source]
        df['country'] = df['country_code']
        rps = self.extracted['google_search_rps']
        df = df.join(rps, rsuffix='_rps')
        # transform here
        td = self.get_target_dataframe()
        td['os'] = df['os']
        td['country'] = df['country_code']
        # workaround for datetime64 validation since `datetime64[ns, UTC]`
        # will raise 'TypeError: data type not understood'
        td['utc_datetime'] = df['day'].astype('datetime64[ns]')
        td['tz'] = df['country'].apply(lambda x: RevenueEtlTask.get_country_tz_str(x))
        td['payout'] = df['event_count'] * df['rps']
        td['payout'] = td['payout'].fillna(0)
        td['sales_amount'] = td['sales_amount'].fillna(0)
        td['source'] = td['source'].fillna('google_search')
        td['currency'] = td['currency'].fillna('USD')
        print(td)
        return td


def main(args):
    srcs = revenue.SOURCES if not args.debug else revenue_dbg.SOURCES
    dests = revenue.DESTINATIONS if not args.debug else revenue_dbg.DESTINATIONS
    task = RevenueEtlTask(args, srcs, revenue.SCHEMA, dests)
    task.run()


if __name__ == "__main__":
    arg_parser = base.get_arg_parser()
    main(arg_parser.parse_args())
