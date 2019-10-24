""" Tests for the time series structure. """
import unittest
import datetime

from gsvi.connection import GoogleConnection
from gsvi.timeseries import SVSeries
from gsvi.catcodes import CategoryCodes


class SVUnivariateTest(unittest.TestCase):
    """ Tests SVSeries with univariate queries. """
    def setUp(self) -> None:
        self.connection = GoogleConnection()

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
        self.assertWarns(UserWarning)
        self.assertEqual(data.max(), 100)

    def test_sv_long(self):
        """
        Tests a single query for a longer period.
        Subtests check the correct normalization - is the max 100 and was it correctly located?
        """
        start = datetime.datetime(year=2017, month=3, day=17)
        end = datetime.datetime(year=2019, month=10, day=18)
        query = {'key': 'apple', 'geo': 'US'}
        series = SVSeries.univariate(self.connection, query, start, end,
                                     category=CategoryCodes.COMPUTERS_ELECTRONICS)
        data = series.get_data()
        with self.subTest('result_normalized'):
            self.assertEqual(data.max(), 100)
        with self.subTest('result_localized_max'):
            max_level = list(series.request_structure)[-1]
            max_query = series.request_structure[max_level]
            self.assertEqual(len(max_query), 1)
            lower, upper = max_query[0]['range']
            self.assertTrue(lower <= data.idxmax() <= upper)


if __name__ == '__main__':
    unittest.main()
