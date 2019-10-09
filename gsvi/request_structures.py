import datetime
import json
from typing import Dict, List, Tuple, Union
from abc import ABC, abstractmethod

import pandas as pd

from .google_connection import GoogleConnection


class BaseStructure(ABC):
    GRANULARITIES = ['DAY', 'HOUR']

    def __init__(self, connection: GoogleConnection, queries: Dict[str, Dict[str, str]], start: datetime.datetime,
                 end: datetime.datetime, granularity='DAY'):
        if not granularity.upper() in self.GRANULARITIES:
            raise ValueError('Invalid granularity!')

        # DAY: minimal time series has two entries
        # HOUR: if interval is shorter than 3 days with HOUR, trends returns sub-hourly data
        delta_min = datetime.timedelta(days=1) if self.granularity == 'DAY' else datetime.timedelta(days=3)
        if start + delta_min > end:
            raise ValueError('Invalid interval! DAY: min end is start + 2 days, HOUR min end is start + 2 days!')

        super().__init__()

        self.connection = connection
        self.queries = queries
        self.start = start
        self.end = end
        self.granularity = granularity.upper()
        self.request_structure = None
        self.data = None

    def __repr__(self):
        return self.request_structure

    def __str__(self):
        return json.dumps(self.request_structure)

    def _build_intervals(self) -> List[Tuple[datetime.datetime, datetime.datetime]]:
        """
        Splits the given interval into tuples of (lower, upper) suited
        for building requests in the requested granularity.
        """
        interval_length = datetime.timedelta(days=90) if self.granularity == 'DAY' else datetime.timedelta(days=7)
        time_pointer = self.end
        intervals = []
        while time_pointer > self.start:
            upper = time_pointer
            time_pointer -= interval_length
            lower = time_pointer
            intervals.append((lower, upper))
        return intervals

    def _get_max_request(self, queries) -> Tuple[datetime.datetime, datetime.datetime]:
        # TODO: get the maximum interval for a single request (max 5 queries
        pass

    @abstractmethod
    def get_data(self, time_gap: int) -> Union[pd.Series, pd.DataFrame]:
        pass


class TSUnivariate(BaseStructure):
    def __init__(self, connection: GoogleConnection, query: Dict[str, Dict[str, str]], start: datetime.datetime,
                 end: datetime.datetime, geo: str, granularity='DAY'):
        super().__init__(connection, query, start, end, granularity=granularity)

    def get_data(self, time_gap=10) -> pd.Series:
        intervals = super()._build_intervals()


class TSCrossSectional(BaseStructure):
    def __init__(self, connection: GoogleConnection, query: Dict[str, Dict[str, str]], start: datetime.datetime,
                 end: datetime.datetime, granularity='DAY'):
        super().__init__(connection, query, start, end, granularity=granularity)

    def get_data(self, time_gap=10) -> pd.DataFrame:
        pass
