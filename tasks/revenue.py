from tasks import base
from configs import revenue
from configs.debug import revenue as revenue_dbg


DEFAULTS = {}



import numpy as np
import pandas as pd
import datetime

class RevenueEtlTask(base.EtlTask):


    def __init__(self, args, sources, destinations):
        super().__init__(args, sources, destinations, 'staging', 'revenue')

    def transform_bukalapak(self, source, config):
        
        # define revenue schema
        revenue_dtype = np.dtype([
            ('source', str),
            ('local_datetime', np.datetime64),
            ('currency', str),
            ('sales_amount', float),
            ('payout', float),
            ('fx_defined1', str),
            ('fx_defined2', str),
            ('fx_defined3', str),
            ('fx_defined4', str),
            ('fx_defined5', str),
            ('conversion_status', str)
        ])
        revenue_df = pd.DataFrame(
            np.empty(0, dtype=revenue_dtype))
        
        
        # transform here
        # https://developers.tune.com/affiliate/affiliate_report-getconversions/
        map_cols = [
            'source',
            'Stat.datetime',
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
        new_df = self.extracted[source]
        new_df = new_df.replace('', np.nan)
        new_df['source'] = source
        
        last_df = self.extracted_base[source]
        last_df['source'] = source
        
        # map cols only
        new_df = new_df[map_cols]
        last_df = last_df[map_cols]
        
        
        
        # check functions
        # new df date range vs. args
        def check_dt_range():
            print('>>> Checking date range...')
            new_df_dt = pd.to_datetime(new_df['Stat.datetime'], format='%Y-%m-%d %H:%M:%S')
            dt_start, dt_end = min(new_df_dt), max(new_df_dt)
            arg_start, arg_end = self.last_month, self.current_date
            assert dt_end <= arg_end, f'>>> From {source}, Max(Date) greater then arg setting.'
            assert dt_start >= arg_start, f'>>> From {source}, Min(Date) less then arg setting.'
            print('>>> Pass date range checking...')
        
        
        # schema match
        def check_schema():
            print('>>> Checking data schema matched...')
            match = list(set(map_cols) & set(new_df.columns))
            not_match = ', '.join(set(map_cols) - set(match))
            assert len(match) == len(map_cols), f'>>> Missing column [ {not_match}  ] from {source}.'
            print('>>> Pass data schema matched...')
        
        
        # invalid null value
        def check_null():
            print('>>> Checking invalid null value...')
            na_cols = new_df.columns[new_df.isna().any()].tolist()
            match = list(set(map_cols[0:5]) & set(na_cols))
            not_null = ', '.join(match)
            assert len(match) == 0, f'>>> From {source}, values in column [ {not_null} ] should not be N/A.'
            print('>>> Pass checking invalid null value...')
            
        
        # which to update (update 交集 append 新資料) key=source'+'Stat.datetime'
        def do_updates_inserts():
            inner = pd.merge(last_df, new_df, on=['source','Stat.datetime'], how='inner')
            print('>>> Done updates...')
            print('>>> Done inserts...')
            print(inner.head(4).to_string())
            
        
        for check in [check_dt_range, check_schema, check_null]:
            try:
                check()
            except AssertionError as e:
                raise e
        print('>>> Done data validation...')

        
        如果沒有 extracted_base 就直接 return
        do_updates_inserts()
        # load_to_gcs()
        
        
        
        

        
        # is ok to update
        # do update
        # do insert
        
        
        new_df.columns = revenue_df.columns
        df = revenue_df.append(new_df, ignore_index = True)
        
        # df = 
        df = df[df['conversion_status']=='approved']
        print(df.head(4).drop(columns='conversion_status').to_string())
        


        return df
    
    
    
    

    def transform_google_search(self, source, config):
        df = self.extracted[source]
        # transform here
        return df


def main(args):
    srcs = revenue.SOURCES if not args.debug else revenue_dbg.SOURCES
    dests = revenue.DESTINATIONS if not args.debug else revenue_dbg.DESTINATIONS
    task = RevenueEtlTask(args, srcs, dests)
    task.run()


if __name__ == "__main__":
    arg_parser = base.get_arg_parser()
    main(arg_parser.parse_args())
