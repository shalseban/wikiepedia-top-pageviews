from datetime import timedelta
import logger
logger = logger.Logger()

class TimeIntervalGenerator(object):
    """Creates Generator to loop through though DateTime Interval"""

    def __init__(self, start_time, end_time):
        """Initialize start and end time
        Parameters
        ----------
        start_time : datetime
            Contains the starting date and hour for the interval
        end_time : datetime
            Contains the ending date and hour for the interval
        """

        self._start_time = start_time
        self._end_time = end_time

    def _filename_url_generator(self):
        """Generator that returns next time and page view data url for that time

        Yields
        ------
        (datetime,str)
            the next datetime and page view data url

        """
        date_format = 'https://dumps.wikimedia.org/other/pageviews/%Y/%Y-%m/pageviews-%Y%m%d-%H0000.gz'
        now = self._start_time
        hour = timedelta(hours=1)
        while now <= self._end_time:
            yield (now, now.strftime(date_format))
            now += hour


class PageViewCounter(object):
    """Couter to remove black list domain,pages and create dictionary from page view data"""

    def __init__(self, dictionary={}):
        """Initialize dictionary and load blacklist set

        Parameters
        ----------
        dictionary : dict, optional
            Stores dictionary or creates new one

        """

        self._dictionary = dictionary
        self._blacklist = Blacklist._get_blacklist_set()
        self._blacklist_count = 0
        self._error_count = 0
        self._total_count = 0

    def _count(self, filename_obj):
        """Create Dictionary from page view data file

        Parameters
        ----------
        filename_obj : obj
            File object to the page view data

        """

        #Open page view data
        with filename_obj.open('r') as each_hour_data_file:

            #Loop through each entry in page view data
            for each_entry in each_hour_data_file:
                self._total_count +=1
                try:
                    #Convert each line to a PageViewEntry
                    page_view_entry = PageViewEntry(each_entry)

                    #Update counter with new page view entry
                    self._update(
                        page_view_entry._domain_page,
                        page_view_entry._count)

                except Exception as e:
                    self._error_count += 1
                    continue
        logger._logger.info('{} entries read'.format(self._total_count))
        logger._logger.info('{} entries blacklisted'.format(self._blacklist_count))
        logger._logger.info('{} errorneous entries found'.format(self._error_count))

    def _update(self, domain_page, page_view):
        """Update dictionary with new entry

        Parameters
        ----------
        domain_page : Tuple
            Contains the Domain, Page pair.
        page_view : int
            Contains page view count.

        """

        #If domain and page in blacklist do not update dictionary
        if domain_page in self._blacklist:
            self._blacklist_count +=1
            return

        #If domain present in dictionary, update counter for domain,page entry else create new entry
        if domain_page in self._dictionary:
            self._dictionary[domain_page] += int(page_view)
        else:
            self._dictionary[domain_page] = int(page_view)

    def __repr__(self):
        return self._dictionary

    def __getitem__(self, domain_page):
        return self._dictionary[domain_page]

    def __len__(self):
        return len(self._dictionary)


class Blacklist(object):
    """Loads the list of domains and pages to be blacklisted"""

    @staticmethod
    def _get_blacklist_set():
        """Load blacklist data from file and create a set for constant lookup"""
        with open('resources/blacklist_domains_and_pages') as blacklist_file:
            blacklist_set = set(tuple(each_domain_page.rstrip().split(' '))
                                for each_domain_page in blacklist_file)
        return blacklist_set


class PageViewEntry(object):
    """Converts each line of page view data into a page view entry of (domain,page) key and count value"""

    def __init__(self, file_line):
        """"Initialize file line and call construct function

        Parameters
        ----------
        file_line : str
            Contains a line from page view data

        """
        self._file_line = file_line
        self._construct_pageview_entry()

    def _construct_pageview_entry(self):
        """Constructs page view entry from page view line"""

        file_line = self._file_line.split()[:-1]
        self._domain_page = (file_line[0], file_line[1])
        self._count = int(file_line[-1])
