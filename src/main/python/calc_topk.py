import arg_parser
import luigi
import tasks
from pybuilder.core import task


def calc_topk():
    parser = arg_parser.ArgParser()
    args = parser._run_argparser()
    print 'Starting calculation with starttime: {} and endtime: {} in aggregate mode {}'.format(
        args.datetime.strftime('%d %b %Y %H'),args.enddatetime.strftime('%d %b %Y %H'),args.agg)
    luigi.build([tasks.CalcTopK(start_time=args.datetime, end_time=args.enddatetime, aggregate=args.agg)],
                local_scheduler=True,log_level='ERROR')
