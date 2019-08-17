from tasks import base
import settings


class RevenueEtlTask(base.EtlTask):

    def __init__(self, args, sources, destinations):
        super().__init__(args, sources, destinations, 'staging')    # define the stage of final output here

    def transform(self):
        super().transform()
        print('Transform revenue data here')


arg_parser = base.get_arg_parser()


def main(args):
    task = RevenueEtlTask(args, settings.SOURCES, settings.DESTINATIONS)
    task.run()


if __name__ == "__main__":
    main(arg_parser.parse_args())
