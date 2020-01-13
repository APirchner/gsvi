""" Holds time series request structure for Google Trends.

The SVSeries class implements an algorithm to get arbitrary-length
time series with values in [0, 100] from GT in the :func:`get_data` method.
This algorithm ensures that GT itself handles the normalization, thus
making the series easier to compare.
It can fetch uni- and multivariate queries.

Example usage::

    gc = GoogleConnection(timeout=10)
    start = datetime.datetime(year=2017, month=1, day=1)
    end = datetime.datetime(year=2019, month=9, day=30)
    series = SVSeries.multivariate(gc,
                [{'key': 'apple', 'geo': 'US'},
                {'key': 'microsoft', 'geo': 'US'}],
                start, end, 'DAY')
    data = series.get_data()

"""

import time
import datetime
import warnings

import json
import math
import random
from typing import Dict, List, Tuple, Union

import pandas as pd

from gsvi.connection import GoogleConnection
from gsvi.catcodes import CategoryCodes


class SVSeries:
    """ Container for uni- or multivariate google search volume time series.

    The main purpose of this class is to get arbitrary-length time series
    data from Google Trends for one or more keywords.

    Attributes:
        connection: The connection to Google Trends.
        queries:
            The user-specified queries dicts as list [{'key': 'word', 'geo': 'country'}, ...].
        bounds:
            The date range for the time series.
            Depending on the location of the maximum and the granularity,
            the lower bound may not hold (see :func:`get_data`).
        category:
            The category for the search volume.
            Possible categories are in the CategoryCodes enum.
        granularity:
            The series granularity, either 'DAY', 'HOUR' or 'MONTH'.
        data:
            The search volume data after the :func:`get_data` call.
        request_structure:
            The query fragments in levels after the :func:`get_data` call,
            showing how the optimum was obtained.
        is_consistent:
            Flag indicating if the data is still consistent
            with the other attributes of the instance.
            This is set to True when :func:`get_data` runs successfully.

    CAUTION: One has to take care when specifying certain time span/granularity combinations.
    Google Trends switches from returning weekly to monthly data
    when the span is >= 1890 days (63 months).
    SVSeries can handle by extending the lower boundary date if necessary.
    The same happens with daily data when the span is longer than
    269 days AND not a multiple of 269 days.
    For hourly data, the switch to minute data happens at < 3 days.
    This weird behavior has changed in the past and might change again in the future!
    See :func:`get_data` for more on how this problem.
    """
    # pylint: disable=too-many-instance-attributes,missing-function-docstring
    # @property causes pylint to count attributes twice

    MAX_FRGMNTS = 5
    GRANULARITIES = {'MONTH': (1890,), 'DAY': (1, 269), 'HOUR': (3, 7)}

    def __init__(self, connection: GoogleConnection, queries: List[Dict[str, str]],
                 bounds: Tuple[datetime.datetime, datetime.datetime], **kwargs):
        self.connection = connection
        self.is_consistent = False
        self.data = None
        self.queries = queries
        self.granularity = kwargs['granularity'] if 'granularity' in kwargs else 'DAY'
        self.category = kwargs['category'] if 'category' in kwargs else CategoryCodes.NONE
        self.bounds = bounds
        self.request_structure = None

    @property
    def connection(self):
        return self._connection

    @property
    def queries(self):
        return self._queries

    @property
    def granularity(self):
        return self._granularity

    @property
    def category(self):
        return self._category

    @property
    def bounds(self):
        return self._bounds

    @property
    def request_structure(self):
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

    @category.setter
    def category(self, category: CategoryCodes):
        self.is_consistent = False
        self._category = category

    @bounds.setter
    def bounds(self, bounds: Tuple[datetime.datetime, datetime.datetime]):
        if bounds[0] < datetime.datetime(year=2004, month=1, day=1):
            raise ValueError('Earliest date is 2004/01/01!')
        if bounds[0] >= datetime.datetime.now():
            raise ValueError('Begin of series has to be in the past!')
        if bounds[1] >= datetime.datetime.now():
            raise ValueError('End of series has to be in the past!')
        if bounds[0] >= bounds[1]:
            raise ValueError('Start date has to be smaller than end date!')
        self.is_consistent = False
        self._bounds = bounds

    @request_structure.setter
    def request_structure(self, request_structure: Dict[str, List]):
        self.is_consistent = False
        self._request_structure = request_structure

    @classmethod
    def univariate(cls, connection: GoogleConnection, query: Dict[str, str],
                   start: datetime.datetime, end: datetime.datetime,
                   **kwargs):
        """
        Builds a univariate search volume series. Initially, the series holds no data.
        Call :func:`get_data` to fill it.

        Args:
            connection:
                The GoogleConnection to use for the requests.
            query:
                The query dict.
            start:
                The start of the series >= 2004/01/01.
            end:
                The end of the series <= now
        Keyword Args:
            granularity:
                The granularity of the series ('DAY', 'HOUR' or 'MONTH').
                Defaults to 'DAY' if not given.
            category:
                Volume for a specfic search category (see :mod:`gsvi.catcodes`).
                Defaults to CategoryCodes.NONE if not given.
        Returns:
            A SVSeries with empty data.
        Raises:
            ValueError
        """
        return cls(connection, [query], (start, end), **kwargs)

    @classmethod
    def multivariate(cls, connection: GoogleConnection, queries: List[Dict[str, str]],
                     start: datetime.datetime, end: datetime.datetime, **kwargs):
        """
        Builds a multivariate search volume series. Initially, the series holds no data.
        Call :func:`get_data` to fill it.

        Args:
            connection:
                The GoogleConnection to use for the requests.
            query:
                A list of query dicts.
            start:
                The start of the series >= 2004/01/01.
            end:
                The end of the series <= now
        Keyword Args:
            granularity:
                The granularity of the series ('DAY', 'HOUR' or 'MONTH').
                Defaults to 'DAY' if not given.
            category:
                Volume for a specfic search category (see :mod:`gsvi.catcodes`).
                Defaults to CategoryCodes.NONE if not given.
        Returns:
            A SVSeries with empty data.
        Raises:
            ValueError
        """
        return cls(connection, queries, (start, end), **kwargs)

    def __repr__(self):
        return self.request_structure

    def __str__(self):
        return json.dumps(self.request_structure, indent=4,
                          separators=(',', ': '), sort_keys=True, default=str)

    def _build_intervals(self) -> List[Tuple[datetime.datetime, datetime.datetime]]:
        """
        Splits the given interval into tuples of (lower, upper) suited
        for building requests in the requested granularity.
        """
        if self.granularity == 'HOUR':
            days = max(min((self.bounds[1] - self.bounds[0]).days,
                           self.GRANULARITIES['HOUR'][1]),
                       self.GRANULARITIES['HOUR'][0])
            interval_length = datetime.timedelta(days=days)
            offset = datetime.timedelta(hours=1)
        elif self.granularity == 'MONTH':
            # no need to split requests for monthly data
            days = max((self.bounds[1] - self.bounds[0]).days,
                       self.GRANULARITIES['MONTH'][0])
            interval_length = datetime.timedelta(days=days)
            offset = datetime.timedelta(days=1)
        else:
            days = max(min((self.bounds[1] - self.bounds[0]).days,
                           self.GRANULARITIES['DAY'][1]),
                       self.GRANULARITIES['DAY'][0])
            interval_length = datetime.timedelta(days=days)
            offset = datetime.timedelta(days=1)

        time_pointer = self.bounds[1]
        intervals = []
        while time_pointer > self.bounds[0]:
            upper = time_pointer
            time_pointer -= interval_length
            intervals.append((time_pointer, upper))
            time_pointer -= offset
        return intervals

    def _get_max_request(
            self, queries
    ) -> Dict[str, Union[str, Tuple[datetime.datetime, datetime.datetime]]]:
        """ Finds the maximum search volume query out of a set of queries """
        response = self.connection.get_timeseries(queries=queries,
                                                  category=self.category,
                                                  granularity=self.granularity)
        for i, query in enumerate(queries):
            if response[i].max() == 100:
                return query
        raise ValueError('Response to queries {0} contains no max of 100'.format(queries))

    def get_data(self, delay=10, force_truncation=False) -> Union[pd.DataFrame, pd.Series]:
        """
        Builds the request structure for the queries and builds requests to Google Trends
        such that the resulting time series values are normalized to [0, 100]. The returned
        data might be extended beyond the lower bound specified in the query. This is
        necessary because GT returns data in different intervals
        depending on the specified range and granularity. One can enforce the
        correct length but might get data not in [0, 100] in case the maximum
        falls into the part that gets truncated.

        Args:
            delay:
                 Put delay seconds between requests to avoid getting banned.
            force_truncation:
                 Truncate to the specified bounds even if the maximal
                 volume (100) does fall into this interval. Default is to not truncate
                 in case the maximum falls into this area.
        Returns:
            The normalized time series as pd.Series (univariate) or pd.Dataframe (multivariate).
        Raises:
            requests.exceptions.RequestException
        Warnings:
            UserWarning: in case truncation is not forced and maximum is in area to be truncated.
        """

        if self.data is not None and self.is_consistent:
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
                    time.sleep(delay + random.uniform(delay * -0.25, delay * 0.25))
                requests.append(layer)
            # Phase 2 - normalize over maximum
            response_flat = []
            for i in range(0, len(requests[0]), self.MAX_FRGMNTS - 1):
                response_flat.extend(self.connection.get_timeseries(
                    queries=requests[0][i:i + self.MAX_FRGMNTS - 1] + requests[-1],
                    category=self.category,
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
        if len(response_stacked) == 1:
            self.data = response_stacked[0]
            max_date = self.data.idxmax()
        else:
            self.data = pd.DataFrame(response_stacked).T
            max_date = self.data.max(axis=1).idxmax()

        if not force_truncation and max_date < self.bounds[0]:
            warnings.warn(
                'Maximal volume is not in specified range. Series is longer than requested!')
        else:
            self.data = self.data[self.bounds[0]:]
        self.is_consistent = True  # data is now consistent
        return self.data
