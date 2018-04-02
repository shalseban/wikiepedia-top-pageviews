import argparse
import datetime


class ArgParser(object):

    def __init__(self):
        self._parser = self._generate_parser()

    def _run_argparser(self):
        args = self._parser.parse_args()
        args.enddatetime = args.datetime if args.enddatetime is None else args.enddatetime
        return args

    def _generate_parser(self):
        parser = argparse.ArgumentParser(description='Compute Top 25 Wikipedia Pages for each sub domain')
        parser.add_argument("-s",
                            "--datetime",
                            help="The Date and Time (Start) - format MM-DD-YYYY-HH",
                            nargs='?',
                            type=self.valid_date,
                            default=datetime.datetime.now())
        parser.add_argument("-e",
                            "--enddatetime",
                            help="The End Date and Time - format MM-DD-YYYY-HH",
                            nargs='?',
                            type=self.valid_date)
        parser.add_argument('-a',
                            '--agg',
                            help='Calculate Aggregated Top 25 between the interval',
                            action='store_true')
        return parser

    def valid_date(self, date_time):
        try:
            return datetime.datetime.strptime(date_time, '%m-%d-%Y-%H')
        except ValueError:
            msg = "Not a valid date: '{0}'.".format(date_time)
            raise argparse.ArgumentTypeError(msg)
