import StringIO
import gzip
import os
import luigi
import pandas as pd
import requests
from util import *
from logger import Logger
from datetime import datetime


logger = Logger()

class FetchHour(luigi.Task):
    """Task to Fetch Data for a single hour from Wiki Repo

    Attributes
    ----------
    file_url : str
        Contains the url to download page view data for the hour

    """

    file_url = luigi.Parameter()

    def run(self):
        logger._logger.info('Fetching Hour Data for {}'.format(datetime.strptime(self.file_url.split('/')[-1],'pageviews-%Y%m%d-%H0000.gz')))

        #Download page view data
        downloaded_data = requests.get(self.file_url, stream=True)
        compressedFile = StringIO.StringIO(downloaded_data.content)

        #Extract and save page view data
        decompressedFile = gzip.GzipFile(fileobj=compressedFile)
        with self.output().open('w') as hour_data:
            hour_data.write(decompressedFile.read())

        logger._logger.info('Fetch Complete')

    def output(self):
        return luigi.LocalTarget("intermediate_data/" + self.file_url.split('/')[-1].replace('.gz', '.txt'))


class ComputeTopK(luigi.Task):
    """Task to group by Domain and compute Top 25 pages for a single hour

        Attributes
        ----------
        date_time : datetime
            Contains the date and hour for which to compute Top 25
        hour_file_name : str
            Contains the url to download page view data for the hour

        """

    date_time = luigi.DateHourParameter()
    hour_file_name = luigi.Parameter()

    def requires(self):

        #Get Page view data
        return FetchHour(self.hour_file_name)

    def run(self):

        logger._logger.info('Computing Top 25 for {}'.format(self.date_time))

        #Initiliaze page view counter and update with page view data
        page_view_counter = PageViewCounter()
        page_view_counter._count(self.input())

        #Get keys from the counter to be used as index for DataFrame
        multiindex = pd.MultiIndex.from_tuples(
            page_view_counter._dictionary.keys(), names=[
                'Domain', 'Page'])
        final_df = pd.DataFrame(
            page_view_counter._dictionary.values(),
            index=multiindex,
            columns=['Count'])
        multiindex = None

        #Group By Domain and get 25 largest from each group and save to output
        final_df.groupby(
            level=0)['Count'].nlargest(25).to_csv(
            self.output().path)
        logger._logger.info('Computing complete')

    def output(self):
        return luigi.LocalTarget(
            'results/' +
            self.date_time.strftime('%m-%d-%Y-%H') +
            "_" +
            self.date_time.strftime('%m-%d-%Y-%H') +
            ".csv")

    def on_success(self):
        os.remove(self.input().path)


class AggregateHoursComputeTopK(luigi.Task):
    """Task to Aggregate Page View data from each hour
    and then group by domain to find Top 25 from each domain

    Attributes
    ----------
    start_time : datetime
        Contains the starting date and hour for the interval
    end_time : datetime
        Contains the ending date and hour for the interval

    """

    start_time = luigi.DateHourParameter()
    end_time = luigi.DateHourParameter()

    def requires(self):

        #Get list of hours between start and end time
        filename_url_list_obj = TimeIntervalGenerator(
            self.start_time, self.end_time)

        #Get page view data for each hour in filename_url_list
        return [FetchHour(each_file) for _,each_file in filename_url_list_obj._filename_url_generator()]

    def run(self):

        logger._logger.info('Aggregating and Computing Top 25 for interval {} to {}'.format(self.start_time,self.end_time))

        # Initiliaze page view counter and update with page view data
        page_view_counter = PageViewCounter()

        #Loop through each hour and update page view counter with page view data
        for each_hour_data in self.input():
            page_view_counter._count(each_hour_data)

        #Get domain and page from counter to use as index in DataFrame
        multiindex = pd.MultiIndex.from_tuples(
            page_view_counter._dictionary.keys(), names=[
                'Domain', 'Page'])

        #Group by domain and get top 25 from each domain to store results in csv
        final_df = pd.DataFrame(
            page_view_counter._dictionary.values(),
            index=multiindex,
            columns=['Count'])
        multiindex = None
        final_df.groupby(
            level=0)['Count'].nlargest(25).to_csv(self.output().path)
        logger._logger.info('Computing complete')

    def output(self):
        return luigi.LocalTarget(
            'results/' +
            self.start_time.strftime('%m-%d-%Y-%H') +
            "_" +
            self.end_time.strftime('%m-%d-%Y-%H') +
            ".csv")

    def on_success(self):
        """Called on completion of running this task to delete used intermediate data"""

        for each_hour_data in self.input():
            os.remove(each_hour_data.path)


class CalcTopK(luigi.Task):
    """Task to Calculate TopK and store summary results

    Attributes
    ----------
    start_time : datetime
        Contains the starting date and hour for the interval
    end_time : datetime
        Contains the ending date and hour for the interval
    end_time : bool
        True if data needs to be aggregated before computing

    """
    start_time = luigi.DateHourParameter()
    end_time = luigi.DateHourParameter()
    aggregate = luigi.BoolParameter()

    def requires(self):
        #If aggregate is True call AggregateHoursComputeTopK task else ComputeTopK
        if self.aggregate:
            return AggregateHoursComputeTopK(self.start_time, self.end_time)
        else:
            filename_url_list_obj = TimeIntervalGenerator(
                self.start_time, self.end_time)
            return [
                ComputeTopK(
                    curr_datetime,
                    each_file) for curr_datetime,
                each_file in filename_url_list_obj._filename_url_generator()]

    def run(self):
        logger._logger.info('Top 25 computation complete')
        self.output().open('w').close()

    def output(self):
        return luigi.LocalTarget('',is_tmp=True)

