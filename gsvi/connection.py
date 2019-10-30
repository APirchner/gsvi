""" Holds GoogleConnection class.

This module provides the interface to Google Trends via the GoogleConnection class.
For now, it only interacts with GT's time series widget via the get_timeseries() method.
Example usage:
    queries = [{...}]
    gc = GoogleConnection()
    ts = gc.get_timeseries(queries)
"""

import json
import datetime
from typing import Dict, List, Tuple, Union

import requests
import pandas as pd

from gsvi.catcodes import CategoryCodes

# type alias
QueryDict = Dict[str, Union[str, Tuple[datetime.datetime, datetime.datetime], CategoryCodes]]


# pylint: disable=too-many-arguments
class GoogleConnection:
    """ Connection to Google Trends.

    Offers the interface to Google Trends. For now, this is limited
    to the time series widget but can be extended easily.
    Usage:
        queries = [{...}]
        gc = GoogleConnection()
        ts = gc.get_timeseries(queries)

    Attributes:
        language: The language, defaults to 'en-US'
        timezone: The timezone in minutes, defaults to 0
        timeout: The timeout for the GET-requests.
        verbose: Print request URLs?
    Raises:
        requests.exceptions.RequestException
    """
    URL_BASE = 'https://trends.google.com/'
    URL_EXPLORE = 'https://trends.google.com/trends/api/explore'
    URL_TS = {
        'SINGLE': 'https://trends.google.com/trends/api/widgetdata/multiline',
        'MULTI': 'https://trends.google.com/trends/api/widgetdata/multirange'
    }

    def __init__(self, language='en-US', timezone=0, timeout=5.0, verbose=False):
        self.timezone = timezone
        self.language = language
        self.timeout = timeout
        self.verbose = verbose
        self.session = requests.Session()
        self._get_request(self.URL_BASE)

    def __del__(self):
        self.session.close()

    def __repr__(self):
        return self.session.headers

    def __str__(self):
        return json.dumps(self.session.headers, indent=4, sort_keys=True, default=str)

    def _get_request(self, url: str, params: Dict = None):
        """ Wraps the session's get request to bundle the request exceptions. """
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as err:
            raise requests.exceptions.RequestException(err)
        except requests.exceptions.ConnectionError as err:
            raise requests.exceptions.RequestException(err)
        except requests.exceptions.Timeout as err:
            raise requests.exceptions.RequestException(err)
        except requests.exceptions.RequestException as err:
            raise requests.exceptions.RequestException(err)

    def _get_explore(self, keywords: List[str],
                     ranges: List[Tuple[datetime.datetime, datetime.datetime]],
                     geos: List[str], category: CategoryCodes, granularity: str) -> dict:
        """ Makes a request to GT Explore API to get payloads and tokens for the widgets. """

        # Transforms the datetime interval into the correct string for the requested granularity
        ranges_str = [' '.join(
            [d.strftime('%Y-%m-%d') if granularity != 'HOUR' \
                 else d.strftime('%Y-%m-%dT%H') for d in dates]) for dates in ranges]

        params = {
            'hl': self.language,
            'tz': self.timezone,
            'req': json.dumps({
                'comparisonItem':
                    [{
                        'keyword': key,
                        'time': time,
                        'geo': geo,
                    } for key, time, geo in zip(keywords, ranges_str, geos)],
                'category': category,
                'property': ''
            })
        }
        response = self._get_request(self.URL_EXPLORE, params=params)
        if self.verbose:
            print(response.url)
        # explore API has 5 leading garbage bytes
        content_raw = json.loads(response.content[5:])['widgets']
        content_dict = {widget['id']: {
            'req': widget['request'] if 'request' in widget else None,
            'token': widget['token'] if 'token' in widget else None
        } for widget in content_raw}
        return content_dict

    def _get_timeseries(self, payload: dict,
                        keyword_num: int, ts_api='SINGLE') -> List[pd.Series]:
        """
        Makes the request to the TIMESERIES widget by using the payload,
        token obtained by _get_explore() and parses the json into pd.Series
        """
        params = {
            'hl': self.language,
            'tz': self.timezone,
            'req': json.dumps(payload['req']),
            'token': payload['token']
        }
        response = self._get_request(self.URL_TS[ts_api], params=params)
        content_raw = json.loads(response.content[5:])['default']['timelineData']

        if ts_api == 'MULTI':
            # rows iterate faster than columns to get lists of columns
            content_parsed = [
                pd.Series({datetime.datetime.fromtimestamp(int(row['columnData'][i]['time'])):
                               row['columnData'][i]['value']
                           for row in content_raw}) for i in range(keyword_num)
            ]
        else:
            content_parsed = [
                pd.Series({datetime.datetime.fromtimestamp(int(row['time'])):
                               row['value'][i]
                           for row in content_raw}) for i in range(keyword_num)
            ]
        return content_parsed

    def get_timeseries(self, queries: List[QueryDict],
                       category=CategoryCodes.NONE, granularity='DAY') -> List[pd.Series]:
        """
        Makes the request to Google Trends for the specified queries.
        This method only does very basic input checks as
        this is handled by the objects using the connection.
        A maximum of 5 queries is supported.
        Args:
            queries:
                The queries as a list of dicts with ranges as tuples of datetime objects.
                Example:
                    [{'key': 'apple', 'geo': 'US',
                    'range': (start, end), 'cat': CategoryCodes.HEALTH},
                    {'key': 'orange', 'geo': 'US',
                    'range': (start, end), 'cat': CategoryCodes.HEALTH}]
            category:
                The category for the query, defaults to CategoryCodes.NONE.
            granularity:
                The step length of the requested series, either 'DAY'/'MONTH' or 'HOUR'.
                Defaults to 'DAY'. Depending on the query ranges,
                the granularity returned by GT might differ.
                Check the SVSeries docs for details.
        Returns:
            A list of pd.Series, one series for each query.
            The values are normalized over the maximal value
            (which is set to 100) over all queries by Trends.
        Raises:
            ValueError
            requests.exceptions.RequestException
        """
        if len(queries) > 5:
            raise ValueError('Too many ({0} > 5) queries!'.format(len(queries)))
        if not all(['key' in query for query in queries]):
            raise KeyError('Every query has to provide a "key"!')
        if not all(['range' in query for query in queries]):
            raise KeyError('Every query has to provide a "range"!')
        if not all(['geo' in query for query in queries]):
            raise KeyError('Every query has to provide a "geo"!')

        keywords = []
        ranges = []
        geos = []
        for query in queries:
            keywords.append(query['key'])
            ranges.append(query['range'])
            geos.append(query['geo'].upper())

        # assign query to its GT API url
        if len(ranges) == 1:
            ts_api = 'SINGLE'
        elif all([ranges[0][0] == ranges[i][0] and ranges[0][1] == ranges[i][1] \
                  for i in range(len(ranges))]):
            # multiple keywords with same range also go to single API!
            ts_api = 'SINGLE'
        else:
            ts_api = 'MULTI'

        widgets = self._get_explore(keywords=keywords, ranges=ranges,
                                    geos=geos, category=category, granularity=granularity)
        series = self._get_timeseries(payload=widgets['TIMESERIES'], keyword_num=len(keywords),
                                      ts_api=ts_api)
        return series
