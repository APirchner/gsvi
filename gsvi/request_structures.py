import time
import datetime
import json
import math
import random
from typing import Dict, List, Tuple, Union
from abc import ABC, abstractmethod

import pandas as pd

from gsvi.google_connection import GoogleConnection


class BaseStructure(ABC):
    GRANULARITIES = ['DAY', 'HOUR']

    def __init__(self, connection: GoogleConnection, queries: List[Dict[str, str]], start: datetime.datetime,
                 end: datetime.datetime, granularity='DAY', delay=10):
        if not granularity.upper() in self.GRANULARITIES:
            raise ValueError('Invalid granularity!')

        # DAY: minimal time series has two entries
        # HOUR: if interval is shorter than 3 days with HOUR, trends returns sub-hourly data
        delta_min = datetime.timedelta(days=1) if granularity == 'DAY' else datetime.timedelta(days=3)
        if start + delta_min > end:
            raise ValueError('Invalid interval! DAY: min end is start + 2 days, HOUR min end is start + 2 days!')

        if start < datetime.datetime(year=2004, month=1, day=1):
            raise ValueError('Earliest date is 2004/01/01!')

        super().__init__()

        self.connection = connection
        self.queries = queries
        self.start = start
        self.end = end
        self.granularity = granularity.upper()
        self.request_structure = None
        self.data = None
        self.delay = delay

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
        time_pointer = self.end
        intervals = []
        while time_pointer > self.start:
            upper = time_pointer
            time_pointer -= interval_length
            lower = time_pointer + offset
            intervals.append((lower, upper))
        return intervals

    def _get_max_request(self, queries) -> Dict[str, Union[str, Tuple[datetime.datetime, datetime.datetime]]]:
        response = self.connection.get_timeseries(queries, self.granularity)
        for i, query in enumerate(queries):
            if response[i].max() == 100:
                return query
        raise ValueError('Response to queries {0} contains no max of 100'.format(queries))

    @abstractmethod
    def get_data(self) -> Union[pd.Series, pd.DataFrame]:
        pass


class TSUnivariate(BaseStructure):
    def __init__(self, connection: GoogleConnection, query: Dict[str, str], start: datetime.datetime,
                 end: datetime.datetime, granularity='DAY', delay=10):
        super().__init__(connection, [query], start, end, granularity, delay)

    def get_data(self) -> pd.Series:
        '''
        Builds the request structure for the query and makes the requests to
        Google Trends such that the resulting time series values are between 0 and 100.
        Returns:
        The normalized time series as pd.Series
        '''
        # PHASE 1 - get global maximum
        requests = list()
        requests.append([{**self.queries[0], **{'range': interval}} for interval in super()._build_intervals()])
        depth = math.ceil(math.log(math.ceil(len(requests[0]) / 5), 5)) + 1
        for i in range(0, depth):
            layer = []
            for j in range(0, len(requests[i]), 5):
                layer.append(super()._get_max_request(requests[i][j:j + 5]))
                # random delay between requests -> delay +/- 25%
                time.sleep(self.delay + random.uniform(-self.delay * 0.25, self.delay * 0.25))
            requests.append(layer)
        # PHASE 2 - normalize over maximum
        response = []
        for i in range(0, len(requests[0]), 4):
            response.extend(self.connection.get_timeseries(
                queries=requests[0][i:i + 4] + requests[-1], granularity=self.granularity
            )[:-1])
        ts = pd.concat(response, verify_integrity=True).sort_index()
        self.data = ts
        self.request_structure = {'layer_{0}'.format(i): requests[i] for i in range(len(requests))}
        return ts


class TSCrossSectional(BaseStructure):

    def __init__(self, connection: GoogleConnection, query: List[Dict[str, str]], start: datetime.datetime,
                 end: datetime.datetime, granularity='DAY', delay=10):
        super().__init__(connection, query, start, end, granularity=granularity, delay=delay)

    def get_data(self) -> pd.DataFrame:
        pass


if __name__ == '__main__':
    gc = GoogleConnection(timeout=10)
    start = datetime.datetime(year=2019, month=5, day=1)
    end = datetime.datetime(year=2019, month=8, day=30)
    ts = TSUnivariate(gc, {'key': 'apple', 'geo': 'US'}, start, end, 'DAY')
    res = ts.get_data()
    print(ts)
