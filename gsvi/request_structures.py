""" Holds time series request structure for Google Trends.

The SVSeries class implements an algorithm to get arbitrary-length
time series with values in [0, 100] from GT in the get_data() method.
This algorithm ensures that GT itself handles the normalization, thus
making the series easier to compare.
It can fetch uni- and multivariate queries.

Example usage:
    gc = GoogleConnection(timeout=10)
    start = datetime.datetime(year=2017, month=1, day=1)
    end = datetime.datetime(year=2019, month=9, day=30)
    series = SVSeries.multivariate(gc,
                [{'key': 'apple', 'geo': 'US'},
                {'key': 'microsoft', 'geo': 'US'}],
                start, end, 'DAY')
    data = series.get_data()

"""

# pylint: disable=too-many-instance-attributes, too-many-arguments

import time
import datetime

import json
import math
import random
from typing import Dict, List, Tuple, Union

import pandas as pd

from gsvi.google_connection import GoogleConnection


class SVSeries:
    """ Container for uni- or multivariate google search volume time series.

    The main purpose of this class is to get arbitrary-length time series
    data from Google Trends for one or more keywords.

    Example usage:
        gc = GoogleConnection(timeout=10)
        start = datetime.datetime(year=2017, month=1, day=1)
        end = datetime.datetime(year=2019, month=9, day=30)
        series = SVSeries.multivariate(gc,
                    [{'key': 'apple', 'geo': 'US'},
                    {'key': 'microsoft', 'geo': 'US'}],
                    start, end, 'DAY')
        data = series.get_data()
    """
    MAX_FRGMNTS = 5
    GRANULARITIES = ['DAY', 'HOUR']

    def __init__(self, connection: GoogleConnection, queries: List[Dict[str, str]],
                 bounds: Tuple[datetime.datetime, datetime.datetime],
                 granularity='DAY', delay=10):
        self.connection = connection
        self.delay = delay
        self.is_consistent = False
        self.data = None
        self.queries = queries
        self.granularity = granularity
        self.bounds = bounds
        self.request_structure = None

    @property
    def connection(self):
        """
        Holds the GoogleConnection.
        """
        return self._connection

    @property
    def queries(self):
        """
        Holds the queries dict.
        Setting this after a call to GT sets the is_consistent flag to False.
        """
        return self._queries

    @property
    def granularity(self):
        """
        Holds the granularity of the series. Either 'DAY' or 'HOUR'.
        Setting this after a call to GT sets the is_consistent flag to False.
        """
        return self._granularity

    @property
    def bounds(self):
        """
        Holds the date range of the time series.
        Setting this after a call to GT sets the is_consistent flag to False.
        """
        return self._bounds

    @property
    def request_structure(self):
        """
        Holds the request structure as a dict after calling get_data().
        Setting this after a call to GT sets the is_consistent flag to False.
        """
        return self._request_structure

    @connection.setter
    def connection(self, connection: GoogleConnection):
        self._connection = connection

    @queries.setter
    def queries(self, queries: List[Dict[str, str]]):
        self.is_consistent = False
        self._queries = queries

    @granularity.setter
    def granularity(self, granularity: str):
        if not granularity.upper() in self.GRANULARITIES:
            raise ValueError('Invalid granularity!')
        self.is_consistent = False
        self._granularity = granularity.upper()

    @bounds.setter
    def bounds(self, bounds: Tuple[datetime.datetime, datetime.datetime]):
        # DAY: minimal time series has two entries
        # HOUR: if interval is shorter than 3 days with HOUR, trends returns sub-hourly data
        delta_min = datetime.timedelta(days=1) if self.granularity == 'DAY' \
            else datetime.timedelta(days=3)

        if bounds[0] + delta_min > bounds[1]:
            raise ValueError(
                'Invalid interval! DAY: min end is start + 2 days, HOUR min end is start + 2 days!')
        if bounds[0] < datetime.datetime(year=2004, month=1, day=1):
            raise ValueError('Earliest date is 2004/01/01!')
        if bounds[0] >= datetime.datetime.now():
            raise ValueError('Begin of series has to be in the past!')
        if bounds[1] >= datetime.datetime.now():
            raise ValueError('End of series has to be in the past!')
        self.is_consistent = False
        self._bounds = bounds

    @request_structure.setter
    def request_structure(self, request_structure):
        self.is_consistent = False
        self._request_structure = request_structure

    @classmethod
    def univariate(cls, connection: GoogleConnection, query: Dict[str, str],
                   start: datetime.datetime, end: datetime.datetime,
                   granularity='DAY', delay=10):
        """
        Builds a univariate search volume series. Initially, the series holds no data.
        Call get_data() to fill it.
        Args:
            connection: The GoogleConnection to use for the requests.
            query: The query dict e.g. {'key': 'apple', 'geo': 'US'}.
            start: The start of the series >= 2004/01/01
            end: The end of the series.
            granularity: The granularity of the requested series.
                         Either 'DAY' or 'HOUR'.
            delay: The average delay between requests in seconds.
        Returns:
            A SVSeries with empty data.
        """
        return cls(connection, [query], (start, end), granularity, delay)

    @classmethod
    def multivariate(cls, connection: GoogleConnection, queries: List[Dict[str, str]],
                     start: datetime.datetime, end: datetime.datetime,
                     granularity='DAY', delay=10):
        """
        Builds a multivariate search volume series. Initially, the series holds no data.
        Call get_data() to fill it.
        Args:
            connection: The GoogleConnection to use for the requests.
            query: The query dict e.g. {'key': 'apple', 'geo': 'US'}.
            start: The start of the series >= 2004/01/01
            end: The end of the series.
            granularity: The granularity of the requested series.
                         Either 'DAY' or 'HOUR'.
            delay: The average delay between requests in seconds.
        Returns:
            A SVSeries with empty data.
        """
        return cls(connection, queries, (start, end), granularity, delay)

    def __repr__(self):
        return self.request_structure

    def __str__(self):
        return json.dumps(self.request_structure, indent=4, sort_keys=True, default=str)

    def _build_intervals(self) -> List[Tuple[datetime.datetime, datetime.datetime]]:
        """
        Splits the given interval into tuples of (lower, upper) suited
        for building requests in the requested granularity.
        """
        interval_length = datetime.timedelta(days=90) if self.granularity == 'DAY' \
            else datetime.timedelta(days=7)
        offset = datetime.timedelta(days=1) if self.granularity == 'DAY' \
            else datetime.timedelta(hours=1)
        time_pointer = self.bounds[1]
        intervals = []
        while time_pointer > self.bounds[0]:
            upper = time_pointer
            time_pointer -= interval_length
            lower = time_pointer + offset
            intervals.append((lower, upper))
        return intervals

    def _get_max_request(self, queries) \
            -> Dict[str, Union[str, Tuple[datetime.datetime, datetime.datetime]]]:
        """ Finds the maximum search volume query out of a set of queries """
        response = self.connection.get_timeseries(queries, self.granularity)
        for i, query in enumerate(queries):
            if response[i].max() == 100:
                return query
        raise ValueError('Response to queries {0} contains no max of 100'.format(queries))

    def get_data(self) -> Union[pd.DataFrame, pd.Series]:
        '''
        Builds the request structure for the queries and builds requests to Google Trends
        such that the resulting time series values are normalized to [0, 100].
        Returns:
            The normalized time series as pd.Series (univariate) or pd.Dataframe (multivariate).
        Raises:
            requests.exceptions.RequestException
        '''

        if self.data is not None:
            # return cached data to avoid getting the data again
            return self.data

        # Preprocessing - flatten queries into the base layer of the request pyramid
        intervals = self._build_intervals()
        requests = [[]]
        for query in self.queries:
            requests[0].extend([{**query, **{'range': interval}} for interval in intervals])

        # shortcut if query range fits into a single request (max 5 fragments per query)
        if len(requests[0]) <= self.MAX_FRGMNTS:
            response_flat = self.connection.get_timeseries(
                queries=requests[0], granularity=self.granularity)
        else:
            # Phase 1 - get global maximum
            depth = math.ceil(math.log(
                math.ceil(len(requests[0]) / self.MAX_FRGMNTS), self.MAX_FRGMNTS)) + 1
            for i in range(0, depth):
                layer = []
                for j in range(0, len(requests[i]), self.MAX_FRGMNTS):
                    layer.append(self._get_max_request(requests[i][j:j + self.MAX_FRGMNTS]))
                    # random delay between requests -> delay +/- 25%
                    time.sleep(self.delay + random.uniform(self.delay * -0.25, self.delay * 0.25))
                requests.append(layer)
            # Phase 2 - normalize over maximum
            response_flat = []
            for i in range(0, len(requests[0]), self.MAX_FRGMNTS - 1):
                response_flat.extend(self.connection.get_timeseries(
                    queries=requests[0][i:i + self.MAX_FRGMNTS - 1] + requests[-1],
                    granularity=self.granularity
                )[:-1])
        # Postprocessing - unflatten normalized responses back into queries
        response_stacked = []
        for i in range(0, len(response_flat), len(intervals)):
            series = pd.concat(response_flat[i:i + len(intervals)],
                               verify_integrity=True).sort_index()
            series.name = requests[0][i + len(intervals) - 1]['key']
            response_stacked.append(series)
        self._request_structure = {'layer_{0}'.format(i): requests[i] for i in range(len(requests))}
        self.is_consistent = True
        if len(response_stacked) == 1:
            self.data = response_stacked[0].loc[self.bounds[0]:]
        else:
            self.data = pd.DataFrame(response_stacked).T.loc[self.bounds[0]:]
        self.is_consistent = True  # data is now consistent
        return self.data
