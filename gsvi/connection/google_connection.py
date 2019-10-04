import json
import datetime
from typing import List, Tuple

import requests
import pandas as pd


class GoogleConnection:
    URL_BASE = 'https://trends.google.com/'
    URL_EXPLORE = 'https://trends.google.com/trends/api/explore'
    URL_TS = {
        'SINGLE': 'https://trends.google.com/trends/api/widgetdata/multiline',
        'MULTI': 'https://trends.google.com/trends/api/widgetdata/multirange'
    }

    def __init__(self, timeout=5.0):
        self.timeout = timeout
        self.hl = 'en-US'
        response = requests.get(self.URL_BASE, timeout=self.timeout)

        if response.status_code is not 200:
            raise requests.exceptions.RequestException(
                'Failed to fetch cookies: <{0} [{1}]>'.format(response.reason, str(response.status_code))
            )

        self.cookies = response.cookies

    def __get_explore(self, session: requests.Session, keywords: List[str], ranges: List[Tuple[datetime.datetime]],
                      geos: List[str], granularity: str) -> dict:
        # transform the datetime interval into the correct string for the requested granularity
        ranges_str = [' '.join(
            [d.strftime('%Y-%m-%d') if granularity == 'DAY' else d.strftime('%Y-%m-%dT%H') for d in dates]
        ) for dates in ranges]

        params = {
            'hl': self.hl,
            'tz': 360,
            'req': json.dumps({
                'comparisonItem':
                    [{
                        'keyword': key,
                        'time': time,
                        'geo': geo,
                    } for key, time, geo in zip(keywords, ranges_str, geos)],
                'category': 0,
                'property': ''
            })
        }

        response = session.get(self.URL_EXPLORE, params=params)
        if response.status_code is not 200:
            raise requests.exceptions.RequestException(
                'Failed to explore: <{0} [{1}]>'.format(response.reason, str(response.status_code))
            )
        # explore API has 5 leading garbage bytes
        content_raw = json.loads(response.content[5:])['widgets']
        content_dict = {widget['id']: {
            'req': widget['request'] if 'request' in widget else None,
            'token': widget['token'] if 'token' in widget else None
        } for widget in content_raw}
        return content_dict

    def __get_timeseries(self, session: requests.Session, payload: dict,
                         keyword_num: int, ts_api='SINGLE') -> List[pd.Series]:
        params = {
            'hl': self.hl,
            'tz': 360,
            'req': json.dumps(payload['req']),
            'token': payload['token']
        }

        response = session.get(self.URL_TS[ts_api], params=params)
        if response.status_code is not 200:
            raise requests.exceptions.RequestException(
                'Failed to explore: <{0} [{1}]>'.format(response.reason, str(response.status_code))
            )
        content_raw = json.loads(response.content[5:])['default']['timelineData']

        if ts_api == 'MULTI':
            # rows iterate faster than columns to get lists of columns
            content_parsed = [
                pd.Series(
                    dict([(
                        datetime.datetime.fromtimestamp(int(row['columnData'][i]['time'])),
                        row['columnData'][i]['value']) for row in content_raw]
                    )
                ) for i in range(keyword_num)
            ]
        else:
            content_parsed = [
                pd.Series(
                    dict([(
                        datetime.datetime.fromtimestamp(int(row['time'])), row['value'][0]
                    ) for row in content_raw]))
            ]

        return content_parsed

    def get_timeseries(self, keywords: List[str], ranges: List[Tuple[datetime.datetime]],
                       geos: List[str], granularity='DAY') -> List[pd.Series]:
        with requests.session() as s:
            s.cookies = self.cookies
            widgets = self.__get_explore(session=s, keywords=keywords, ranges=ranges,
                                         geos=geos, granularity=granularity)
            ts = self.__get_timeseries(session=s, payload=widgets['TIMESERIES'], keyword_num=len(keywords),
                                       ts_api='SINGLE' if len(keywords) == 1 else 'MULTI')
        return ts
