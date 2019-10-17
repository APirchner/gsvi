import unittest

from gsvi.google_connection import GoogleConnection


class GoogleConnectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.gc = GoogleConnection()

    def test_request_count(self):
        queries = [{str(i): str(i)} for i in range(10)]
        self.assertRaises(ValueError, self.gc.get_timeseries, queries)


if __name__ == '__main__':
    unittest.main()
