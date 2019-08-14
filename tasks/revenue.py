from tasks import base
import settings


class RevenueEtlTask(base.EtlTask):

    def transform(self):
        super().transform()
        print('Transform revenue data here')


arg_parser = base.get_arg_parser()


def main(args):
    task = RevenueEtlTask(args.date, args.period, settings.SOURCES)
    task.extract()


if __name__ == "__main__":
    main(arg_parser.parse_args())
