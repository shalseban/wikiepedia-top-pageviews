import unittest
import datetime
import logger

logger=logger.Logger
from util import *

class TimeIntervalGeneratorTest(unittest.TestCase):

    def test__filename_url_generator(self):
        start_date = datetime.datetime.strptime('2016-05-31-23','%Y-%m-%d-%H')
        end_date = datetime.datetime.strptime('2016-06-01-01', '%Y-%m-%d-%H')
        time_int_gen = TimeIntervalGenerator(start_date,end_date)
        time_list = ['https://dumps.wikimedia.org/other/pageviews/2016/2016-05/pageviews-20160531-230000.gz',
                     'https://dumps.wikimedia.org/other/pageviews/2016/2016-06/pageviews-20160601-000000.gz',
                     'https://dumps.wikimedia.org/other/pageviews/2016/2016-06/pageviews-20160601-010000.gz']

        get_list =[val for _,val in time_int_gen._filename_url_generator()]

        self.assertListEqual(time_list,get_list)

class PageViewEntryTest(unittest.TestCase):

    def test__construct_pageview_entry(self):

        test_page_view_line = 'a.b this_is_a_rigorous_test 1 200000'

        page_view_entry = PageViewEntry(test_page_view_line)
        self.assertEqual(page_view_entry._domain_page,('a.b','this_is_a_rigorous_test'))
        self.assertEqual(page_view_entry._count,1)

