import datetime
import json
from typing import List, Tuple
from abc import ABC, abstractmethod

from .google_connection import GoogleConnection


class BaseStructure(ABC):
    GRANULARITIES = ['DAY', 'HOUR']

    def __init__(self, connection: GoogleConnection, keywords: List[str], start: datetime.datetime,
                 end: datetime.datetime, geos: List[str], granularity='DAY'):
        if len(keywords) != len(geos):
            raise ValueError('Length of keyword list not equal to geos!')
        elif not granularity.upper() in self.GRANULARITIES:
            raise ValueError('Invalid granularity!')

        # DAY: minimal time series has two entries
        # HOUR: if interval is shorter than 3 days with HOUR, trends returns sub-hourly data
        delta_min = datetime.timedelta(days=1) if self.granularity == 'DAY' else datetime.timedelta(days=3)
        if start + delta_min > end:
            raise ValueError('Invalid interval! DAY: min end is start + 2 days, HOUR min end is start + 2 days!')

        super().__init__()

        self.connection = connection
        self.keywords = keywords
        self.start = start
        self.end = end
        self.geos = geos
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
        interval_length = datetime.timedelta(days=30) if self.granularity == 'DAY' else datetime.timedelta(days=7)
        time_pointer = self.end
        intervals = []
        while time_pointer > self.start:
            upper = time_pointer
            time_pointer -= interval_length
            lower = time_pointer
            intervals.append((lower, upper))
        return intervals

    @abstractmethod
    def get_data(self):
        pass


class TSUnivariate(BaseStructure):
    def __init__(self, connection: GoogleConnection, keyword: str, start: datetime.datetime,
                 end: datetime.datetime, geo: str, granularity='DAY'):
        super().__init__(connection, [keyword], start, end, [geo], granularity=granularity)

    def get_data(self):
        pass


class TSCrossSectional(BaseStructure):
    def __init__(self, connection: GoogleConnection, keywords: List[str], start: datetime.datetime,
                 end: datetime.datetime, geos: List[str], granularity='DAY'):
        super().__init__(connection, keywords, start, end, geos, granularity=granularity)

    def get_data(self):
        pass
