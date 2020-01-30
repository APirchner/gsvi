""" Holds related queries for Google Trends searches.

RelatedQueries allows to specify one or multiple queries and
fetches the related queries from Google Trends. The returned data contains
the related queries for each query, the corresponding value and
the link to the Google Trends search for the related query.

Example usage::

    gc = GoogleConnection(timeout=10)
    start = datetime.datetime(year=2017, month=1, day=1)
    end = datetime.datetime(year=2019, month=9, day=30)
    related = RelatedQueries.multiple(gc,
                [{'key': 'apple', 'geo': 'US'},
                {'key': 'microsoft', 'geo': 'US'}],
                start, end)
    data = related.get_data()

"""

import datetime
from typing import Dict, List, Tuple

import pandas as pd

from gsvi.connection import GoogleConnection
from gsvi.catcodes import CategoryCodes


class RelatedQueries:
    """ Container for Google Trends related queries.

    The main purpose of this class is to get and hold related query data for one or
    multiple user-specified queries (i.e. keyword and region).

    Attributes:
        connection: The connection to Google Trends.
        queries:
            The user-specified queries dicts as list [{'key': 'word', 'geo': 'country'}, ...].
        bounds:
            The date range for the request.
        category:
            The category for the search volume.
            Possible categories are in the CategoryCodes enum.
        data:
            The related-queries data after the :func:`get_data` call.
        is_consistent:
            Flag indicating if the data is still consistent
            with the other attributes of the instance.
            This is set to True when :func:`get_data` runs successfully.
    """

    # pylint: disable=too-many-instance-attributes,missing-function-docstring
    # @property causes pylint to count attributes twice

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
               start: datetime.datetime, end: datetime.datetime, **kwargs):
        """
        Builds a RelatedQueries object for a single query. Initially, the series holds no data.
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
            category:
                Volume for a specfic search category (see :mod:`gsvi.catcodes`).
                Defaults to CategoryCodes.NONE if not given.
        Returns:
            A RelatedQueries object with empty data.
        Raises:
            ValueError
        """
        return cls(connection, [query], (start, end), **kwargs)

    @classmethod
    def multiple(cls, connection: GoogleConnection, queries: List[Dict[str, str]],
                 start: datetime.datetime, end: datetime.datetime, **kwargs):
        """
        Builds a RelatedQueries object for multiple queries. Initially, the series holds no data.
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
            category:
                Volume for a specfic search category (see :mod:`gsvi.catcodes`).
                Defaults to CategoryCodes.NONE if not given.
        Returns:
            A RelatedQueries object with empty data.
        Raises:
            ValueError
        """
        return cls(connection, queries, (start, end), **kwargs)

    def get_data(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        '''
        Gets the related queries for the specified queries from Google Trends.
        For each key, the returned data contains the list of related queries,
        their values and links to Google Trends. For information on how to interpret the values,
        please refer to Google Trends.
        A call to Google Trends is only made if it is the first call or the cached data is
        inconsistent with the other fields of the object.
        Returns:
            A dict of dicts with top and rising related queries for each passed query.
        Raises:
            requests.exceptions.RequestException
        '''
        if self.data is not None and self.is_consistent:
            # return cached data to avoid getting the data again
            return self.data
        # add bounds to every query
        queries_bounds = [{**query, 'range': self.bounds} for query in self.queries]

        self.data = {}
        for i in range(0, len(self.queries), 5):
            self.data = {**self.data, **self.connection.get_related_queries(
                queries=queries_bounds[i:min(i + 4, len(queries_bounds))])}
            self.is_consistent = True
        return self.data
