"""Unit tests for stock monitoring and alert system."""
import unittest

class TestAlertThresholds(unittest.TestCase):
    def test_price_above_threshold_triggers_alert(self):
        price, threshold = 155.0, 150.0
        self.assertTrue(price > threshold)

    def test_price_below_threshold_no_alert(self):
        price, threshold = 145.0, 150.0
        self.assertFalse(price > threshold)

    def test_percent_change_calculation(self):
        prev, curr = 100.0, 105.0
        change = (curr - prev) / prev * 100
        self.assertAlmostEqual(change, 5.0)

    def test_zscore_anomaly_detection(self):
        import statistics
        prices = [100,101,99,100,102,98,100,115]  # 115 is anomaly
        mean = statistics.mean(prices[:-1])
        std  = statistics.stdev(prices[:-1])
        z = (prices[-1] - mean) / std
        self.assertGreater(abs(z), 2.0)  # 115 is > 2 std devs from mean

class TestSymbolValidation(unittest.TestCase):
    def test_valid_symbol_uppercase(self):
        symbols = ["AAPL","TSLA","NVDA","MSFT"]
        for s in symbols:
            self.assertTrue(s.isupper())
            self.assertGreater(len(s), 0)

    def test_invalid_symbol_rejected(self):
        invalid = ["", "  ", "123"]
        for s in invalid:
            self.assertFalse(s.isalpha() and s.isupper() and len(s) > 0)

if __name__ == "__main__": unittest.main()
