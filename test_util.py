import unittest
from util import normalize_sales, setup_logger
import logging

logger = logging.getLogger("store_sales")
logger.addHandler(setup_logger(logfile="test.log"))

class TestNormalizeSales(unittest.TestCase):
    def test_basic_numbers(self):
        self.assertEqual(normalize_sales("10 ksek"), 10000)
        self.assertEqual(normalize_sales("1000"), 1000)
        self.assertEqual(normalize_sales("0,5 ksek"), 500)
        self.assertEqual(normalize_sales("annat"), None)
        self.assertEqual(normalize_sales("4 tkr"), None)
        self.assertEqual(normalize_sales("4 sek"), None)
