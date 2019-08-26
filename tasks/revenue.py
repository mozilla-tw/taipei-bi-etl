from tasks import base
from configs import revenue
from configs.debug import revenue as revenue_dbg


DEFAULTS = {}


class RevenueEtlTask(base.EtlTask):

    def __init__(self, args, sources, destinations):
        super().__init__(args, sources, destinations, 'staging', 'revenue')

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
