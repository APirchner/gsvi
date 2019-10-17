import time
import datetime
import json
import math
import random
from typing import Dict, List, Tuple, Union

import pandas as pd

from gsvi.google_connection import GoogleConnection


class SVSeries(object):
    MAX_FRGMNTS = 5
    GRANULARITIES = ['DAY', 'HOUR']

    def __init__(self, connection: GoogleConnection, queries: List[Dict[str, str]],
                 range: Tuple[datetime.datetime, datetime.datetime], granularity='DAY', delay=10):

        self.connection = connection
        self.delay = delay
        self.is_consistent = False
        self._data = None
        self._queries = queries
        self._range = range
        self._granularity = granularity.upper()
        self._request_structure = None

    @property
    def queries(self):
        return self._queries

    @property
    def range(self):
        return self._range

    @property
    def granularity(self):
        return self._granularity

    @property
    def request_structure(self):
        return self._request_structure

    @queries.setter
    def queries(self, queries: List[Dict[str, str]]):
        self.is_consistent = False
        self._queries = queries

    @range.setter
    def range(self, range: Tuple[datetime.datetime, datetime.datetime]):
        # DAY: minimal time series has two entries
        # HOUR: if interval is shorter than 3 days with HOUR, trends returns sub-hourly data
        delta_min = datetime.timedelta(days=1) if self.granularity == 'DAY' else datetime.timedelta(days=3)
        if self.range[0] + delta_min > self.range[1]:
            raise ValueError('Invalid interval! DAY: min end is start + 2 days, HOUR min end is start + 2 days!')

        if self.range[0] < datetime.datetime(year=2004, month=1, day=1):
            raise ValueError('Earliest date is 2004/01/01!')
        self.is_consistent = False
        self._range = range


    @granularity.setter
    def granularity(self, granularity: str):
        if not granularity.upper() in self.GRANULARITIES:
            raise ValueError('Invalid granularity!')
        self.is_consistent = False
        self._granularity = granularity

    @request_structure.setter
    def request_structure(self, request_structure):
        self.is_consistent = False
        self._request_structure = request_structure

    @classmethod
    def univariate(cls, connection: GoogleConnection, query: Dict[str, str], start: datetime.datetime,
                   end: datetime.datetime, granularity='DAY', delay=10):
        return cls(connection, [query], (start, end), granularity, delay)

    @classmethod
    def multivariate(cls, connection: GoogleConnection, queries: List[Dict[str, str]], start: datetime.datetime,
                     end: datetime.datetime, granularity='DAY', delay=10):
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
        interval_length = datetime.timedelta(days=90) if self.granularity == 'DAY' else datetime.timedelta(days=7)
        offset = datetime.timedelta(days=1) if self.granularity == 'DAY' else datetime.timedelta(hours=1)
        time_pointer = self.range[1]
        intervals = []
        while time_pointer > self.range[0]:
            upper = time_pointer
            time_pointer -= interval_length
            lower = time_pointer + offset
            intervals.append((lower, upper))
        return intervals

    def _get_max_request(self, queries) -> Dict[str, Union[str, Tuple[datetime.datetime, datetime.datetime]]]:
        """ Finds the maximum search volume query out of a set of queries """
        response = self.connection.get_timeseries(queries, self.granularity)
        for i, query in enumerate(queries):
            if response[i].max() == 100:
                return query
        raise ValueError('Response to queries {0} contains no max of 100'.format(queries))

    def get_data(self) -> Union[pd.DataFrame, pd.Series]:
        '''
        Builds the request structure for the queries and makes the requests to
        Google Trends such that the resulting time series values are between 0 and 100.
        Returns:
        The normalized time series as pd.Series (univariate) or pd.Dataframe (multivariate)
        '''

        if self._data is not None:
            # return cached data to avoid getting the data again
            return self._data

        # Preprocessing - flatten queries into the base layer of the request pyramid
        intervals = self._build_intervals()
        requests = [[]]
        for query in self.queries:
            requests[0].extend([{**query, **{'range': interval}} for interval in intervals])

        # shortcut if query range fits into a single request (max 5 fragments per query)
        if len(requests[0]) <= self.MAX_FRGMNTS:
            response_flat = self.connection.get_timeseries(queries=requests[0], granularity=self.granularity)
        else:
            # Phase 1 - get global maximum
            depth = math.ceil(math.log(math.ceil(len(requests[0]) / self.MAX_FRGMNTS), self.MAX_FRGMNTS)) + 1
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
                    queries=requests[0][i:i + self.MAX_FRGMNTS - 1] + requests[-1], granularity=self.granularity
                )[:-1])
        # Postprocessing - unflatten normalized responses back into queries
        response_stacked = []
        for i in range(0, len(response_flat), len(intervals)):
            series = pd.concat(response_flat[i:i + len(intervals)], verify_integrity=True).sort_index()
            series.name = requests[0][i + len(intervals) - 1]['key']
            response_stacked.append(series)
        self._request_structure = {'layer_{0}'.format(i): requests[i] for i in range(len(requests))}
        self.is_consistent = True
        if len(response_stacked) == 1:
            self._data = response_stacked[0].loc[self.range[0]:]
        else:
            self._data = pd.DataFrame(response_stacked).T.loc[self.range[0]:]
        return self._data


if __name__ == '__main__':
    gc = GoogleConnection(timeout=10)
    start = datetime.datetime(year=2017, month=1, day=1)
    end = datetime.datetime(year=2019, month=9, day=30)
    ts = SVSeries.multivariate(gc, [{'key': 'apple', 'geo': 'US'}, {'key': 'microsoft', 'geo': 'US'}],
                             start, end, 'DAY')
    res = ts.get_data()
    print(ts)
