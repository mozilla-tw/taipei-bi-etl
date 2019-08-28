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
        df = self.extracted[source]
        # transform here
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
        td['local_datetime'] = df['day'].astype('datetime64[ns]')
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
