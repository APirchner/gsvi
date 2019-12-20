import datetime
from typing import Dict, List, Tuple

from connection import GoogleConnection
from catcodes import CategoryCodes


class RelatedQueries:
    def __init__(self, connection: GoogleConnection, queries: List[Dict[str, str]],
                 bounds: Tuple[datetime.datetime, datetime.datetime], **kwargs):
        self.connection = connection
        self.is_consistent = False
        self.data = None
        self.queries = queries
        self.category = kwargs['category'] if 'category' in kwargs else CategoryCodes.NONE
        self.fragments = kwargs['fragments'] if 'fragments' in kwargs else 1
        self.bounds = bounds

    @property
    def connection(self):
        return self._connection

    @property
    def queries(self):
        return self._queries

    @property
    def category(self):
        return self._category

    @property
    def fragments(self):
        return self._fragments

    @property
    def bounds(self):
        return self._bounds

    @connection.setter
    def connection(self, connection: GoogleConnection):
        self._connection = connection

    @queries.setter
    def queries(self, queries: List[Dict[str, str]]):
        self.is_consistent = False
        self._queries = queries

    @category.setter
    def category(self, category: CategoryCodes):
        self.is_consistent = False
        self._category = category

    @fragments.setter
    def fragments(self, fragments: int):
        if fragments < 1:
            raise ValueError('Number of fragments has to be positive')
        self.is_consistent = False
        self._fragments = fragments

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

    @classmethod
    def single(cls, connection: GoogleConnection, query: Dict[str, str],
               bounds: Tuple[datetime.datetime, datetime.datetime], **kwargs):
        pass

    @classmethod
    def multiple(cls, connection: GoogleConnection, query: List[Dict[str, str]],
                 bounds: Tuple[datetime.datetime, datetime.datetime], **kwargs):
        pass

    def get_data(self):
        pass
