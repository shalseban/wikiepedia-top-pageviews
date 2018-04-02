from sys import path
path.append("src/main/python")

import arg_parser
import luigi
import tasks
import datetime,logger

logger = logger.Logger()



parser = arg_parser.ArgParser()
args = parser._run_argparser()
print 'Starting calculation with starttime: {} and endtime: {} in aggregate mode {}'.format(
    args.datetime.strftime('%d %b %Y %H'),args.enddatetime.strftime('%d %b %Y %H'),args.agg)
now = datetime.datetime.now()
luigi.build([tasks.CalcTopK(start_time=args.datetime, end_time=args.enddatetime, aggregate=args.agg)],
            local_scheduler=True,log_level='ERROR')
logger._logger.info('Computed in {}s'.format((datetime.datetime.now()-now).total_seconds()))