from tasks import base
import settings


class RevenueEtlTask(base.EtlTask):

    def __init__(self, args, sources, destinations):
        super().__init__(args, sources, destinations, 'staging')

    def transform_bukalapak(self, source, config):
        df = self.extracted[source]
        # transform here
        return df

    def transform_google_search(self, source, config):
        df = self.extracted[source]
        # transform here
        return df


arg_parser = base.get_arg_parser()


def main(args):
    task = RevenueEtlTask(args, settings.SOURCES, settings.DESTINATIONS)
    task.run()


if __name__ == "__main__":
    main(arg_parser.parse_args())
