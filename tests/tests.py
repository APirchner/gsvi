""" Tests for the Google Trends connection and the request structure. """

import unittest
import datetime

from gsvi.google_connection import GoogleConnection


class GoogleConnectionTest(unittest.TestCase):
    """ Tests the GoogleConnection. """

    def setUp(self) -> None:
        """ sets the connection up. """
        self.connection = GoogleConnection()

    def test_count(self):
        """ Simple query count check. """
        queries = [{str(i): str(i)} for i in range(10)]
        self.assertRaises(ValueError, self.connection.get_timeseries, queries)

    def test_single_short_daily(self):
        """ Tests a single query for 2 days. """
        start = datetime.datetime(year=2019, month=9, day=1)
        end = datetime.datetime(year=2019, month=9, day=2)
        queries = [{'key': 'apple', 'geo': 'US',
                    'range': (start, end)}]
        result = self.connection.get_timeseries(queries, granularity='DAY')
        with self.subTest('result_count'):
            self.assertEqual(len(queries), len(result))
        with self.subTest('result_length'):
            self.assertEqual((end - start).days + 1, len(result[0].index))
        with self.subTest('result_normalized'):
            self.assertEqual(result[0].max(), 100)
        with self.subTest('result_start'):
            self.assertEqual(result[0].index.date[0], start.date())
        with self.subTest('result_end'):
            self.assertEqual(result[0].index.date[-1], end.date())

    def test_multi_long_daily(self):
        """ Tests multiple queries for 30 days. """
        start = datetime.datetime(year=2019, month=9, day=1)
        end = datetime.datetime(year=2019, month=10, day=1)
        queries = [{'key': 'apple', 'geo': 'US',
                    'range': (start, end)},
                   {'key': 'orange', 'geo': 'US',
                    'range': (start, end)},
                   {'key': 'banana', 'geo': 'US',
                    'range': (start, end)},
                   {'key': 'kiwi', 'geo': 'US',
                    'range': (start, end)},
                   {'key': 'strawberry', 'geo': 'US',
                    'range': (start, end)}
                   ]
        result = self.connection.get_timeseries(queries, granularity='DAY')
        with self.subTest('result_count'):
            self.assertEqual(len(queries), len(result))
        with self.subTest('result_length'):
            for series in result:
                self.assertEqual((end - start).days + 1, len(series.index))
        with self.subTest('result_normalized'):
            self.assertGreaterEqual(sum([result[i].max() == 100 for i in range(len(result))]), 1)
        with self.subTest('result_start'):
            for series in result:
                self.assertEqual(series.index.date[0], start.date())
        with self.subTest('result_start'):
            for series in result:
                self.assertEqual(series.index.date[0], start.date())


if __name__ == '__main__':
    unittest.main()
