""" Tests for the time series structure. """
import unittest
import datetime
import math

from gsvi.connection import GoogleConnection
from gsvi.timeseries import SVSeries
from gsvi.catcodes import CategoryCodes


class test_SVUnivariate(unittest.TestCase):
    """ Tests SVSeries with univariate queries. """

    def setUp(self) -> None:
        self.connection = GoogleConnection(timeout=30)

    def test_sv_short(self):
        """
        Tests a single query for 2 days worldwide.
        Checks for correct normalization and also if it warns that the series was not truncated.
        """
        start = datetime.datetime(year=2017, month=3, day=17)
        end = datetime.datetime(year=2017, month=3, day=18)
        query = {'key': 'apple', 'geo': ''}
        series = SVSeries.univariate(self.connection, query, start, end)
        data = series.get_data()
        with self.subTest('result_normalized'):
            self.assertTrue(data.max() == 100)
        with self.subTest('result_daily'):
            self.assertEqual(len(data), (end - start).days + 1)

    def test_sv_short_month(self):
        """
        Tests a single query for 1 month worldwide.
        Checks for correct normalization and also if it warns that the series was not truncated.
        """
        start = datetime.datetime(year=2017, month=5, day=1)
        end = datetime.datetime(year=2017, month=5, day=2)
        query = {'key': 'apple', 'geo': ''}
        series = SVSeries.univariate(self.connection, query, start, end, granularity='MONTH')
        data = series.get_data()
        with self.subTest('result_warning'):
            self.assertWarns(UserWarning)
        with self.subTest('result_normalized'):
            self.assertTrue(data.max() == 100)
        with self.subTest('result_monthly'):
            self.assertEqual(data.shape[0], math.ceil(series.GRANULARITIES['MONTH'][0] / 30))

    def test_sv_long(self):
        """
        Tests a single query for a longer period for daily data.
        Subtests check the correct normalization - is the max 100 and was it correctly located?
        """
        start = datetime.datetime(year=2009, month=3, day=17)
        end = datetime.datetime(year=2019, month=10, day=18)
        query = {'key': 'apple', 'geo': 'US'}
        series = SVSeries.univariate(self.connection, query, start, end,
                                     category=CategoryCodes.COMPUTERS_ELECTRONICS)
        data = series.get_data()
        with self.subTest('result_normalized'):
            self.assertTrue(data.max() == 100)
        with self.subTest('result_daily'):
            self.assertEqual(data.shape[0], (end - start).days + 1)
        with self.subTest('result_localized_max'):
            max_level = list(series.request_structure)[-1]
            max_query = series.request_structure[max_level]
            self.assertEqual(len(max_query), 1)
            lower, upper = max_query[0]['range']
            self.assertTrue(lower <= data.idxmax() <= upper)

    def test_sv_long_month(self):
        """
        Tests a single query for a longer period for monthly data.
        Subtests check the correct normalization and if the data is really monthly.
        """
        start = datetime.datetime(year=2010, month=4, day=1)
        end = datetime.datetime(year=2019, month=9, day=30)
        query = {'key': 'apple', 'geo': 'US'}
        series = SVSeries.univariate(self.connection, query, start, end,
                                     category=CategoryCodes.COMPUTERS_ELECTRONICS,
                                     granularity='MONTH')
        data = series.get_data()
        with self.subTest('result_normalized'):
            self.assertEqual(data.max(), 100)
        with self.subTest('result_monthly'):
            self.assertEqual(len(data), math.floor((end - start).days / 30) - 1)


class test_SVMultivariate(unittest.TestCase):
    """ Tests SVSeries with multivariate queries. """

    def setUp(self) -> None:
        self.connection = GoogleConnection(timeout=30)

    def test_sv_short_multi(self):
        """
        Tests 2 queries query for 2 days in the US.
        Checks for correct normalization and also if data is really daily.
        """
        start = datetime.datetime(year=2017, month=3, day=17)
        end = datetime.datetime(year=2017, month=3, day=18)
        queries = [{'key': 'apple', 'geo': 'US'}, {'key': 'orange', 'geo': 'US'}]
        series = SVSeries.multivariate(self.connection, queries, start, end)
        data = series.get_data(force_truncation=True)
        with self.subTest('result_normalized'):
            self.assertTrue(any(data.max() == 100))
        with self.subTest('result_daily'):
            self.assertEqual(data.shape[0], (end - start).days + 1)

    def test_sv_short_month_multi(self):
        """
        Tests 2 queries for 2 months worldwide.
        Subtests check the correct normalization and if the data is really monthly.
        """
        start = datetime.datetime(year=2019, month=8, day=1)
        end = datetime.datetime(year=2019, month=9, day=1)
        queries = [{'key': 'apple', 'geo': 'US'}, {'key': 'google', 'geo': 'US'},
                   {'key': 'microsoft', 'geo': 'US'}, {'key': 'oracle', 'geo': 'US'},
                   {'key': 'facebook', 'geo': 'US'}, {'key': 'uber', 'geo': 'US'}]
        series = SVSeries.multivariate(self.connection, queries, start, end,
                                       category=CategoryCodes.COMPUTERS_ELECTRONICS,
                                       granularity='MONTH')
        data = series.get_data()
        with self.subTest('result_normalized'):
            self.assertTrue(any(data.max() == 100))
        with self.subTest('result_monthly'):
            self.assertEqual(data.shape[0], math.ceil((end - start).days / 30))

    def test_sv_long_multi(self):
        """
        Tests 2 queries for 2 days worldwide.
        Checks for correct normalization and also if data is really daily.
        """
        start = datetime.datetime(year=2009, month=3, day=17)
        end = datetime.datetime(year=2019, month=10, day=18)
        queries = [{'key': 'apple', 'geo': 'US'}, {'key': 'orange', 'geo': 'US'}]
        series = SVSeries.multivariate(self.connection, queries, start, end)
        data = series.get_data(force_truncation=True)
        with self.subTest('result_normalized'):
            self.assertTrue(any(data.max() == 100))
        with self.subTest('result_daily'):
            self.assertEqual(data.shape[0], (end - start).days + 1)
        with self.subTest('result_localized_max'):
            max_level = list(series.request_structure)[-1]
            max_query = series.request_structure[max_level]
            self.assertEqual(len(max_query), 1)
            max_key = max_query[0]['key']
            lower, upper = max_query[0]['range']
            self.assertTrue(data[max_key].max() == 100)
            self.assertTrue(lower <= data[max_key].idxmax() <= upper)

    def test_sv_long_month_multi(self):
        """
        Tests 2 queries for a longer period of monthly data.
        Subtests check the correct normalization and if the data is really monthly.
        """
        start = datetime.datetime(year=2010, month=4, day=1)
        end = datetime.datetime(year=2019, month=9, day=1)
        queries = [{'key': 'apple', 'geo': 'US'}, {'key': 'orange', 'geo': 'US'}]
        series = SVSeries.multivariate(self.connection, queries, start, end,
                                       category=CategoryCodes.COMPUTERS_ELECTRONICS,
                                       granularity='MONTH')
        data = series.get_data()
        with self.subTest('result_normalized'):
            self.assertTrue(any(data.max() == 100))
        with self.subTest('result_monthly'):
            self.assertEqual(data.shape[0], math.floor((end - start).days / 30))