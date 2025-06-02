import unittest

from src.pipeline import data_cleaner


class TestDataCleaner(unittest.TestCase):
    def test_clean_and_parse_data_valid(self):
        """Test clean_and_parse_data parses a valid raw string to a list of dicts."""
        raw = "[{'Timelogged': '05/29/2025 12:00:00 AM', 'val': 1}]"
        result = data_cleaner.clean_and_parse_data(raw)
        self.assertIsInstance(result, list)
        self.assertEqual(result[0]["Timelogged"], "05/29/2025 12:00:00 AM")

    def test_clean_and_parse_data_script_removal(self):
        """Test clean_and_parse_data removes <script> tags from the raw data before parsing."""
        raw = "<script>alert('x')</script>[{'Timelogged': '05/29/2025 12:00:00 AM', 'val': 1}]"
        result = data_cleaner.clean_and_parse_data(raw)
        self.assertEqual(result[0]["val"], 1)

    def test_clean_and_parse_data_malformed(self):
        """Test clean_and_parse_data raises Exception on malformed input that cannot be parsed."""
        raw = "not a list"
        with self.assertRaises(Exception):
            data_cleaner.clean_and_parse_data(raw)

    def test_clean_data_valid(self):
        """Test clean_data returns a list of dicts for valid input data."""
        raw = "[{'Timelogged': '05/29/2025 12:00:00 AM', 'val': 1}]"
        result = data_cleaner.clean_data(raw, "05-29-2025", "daily")
        self.assertIsInstance(result, list)

    def test_clean_data_malformed(self):
        """Test clean_data returns 0 on malformed input data."""
        raw = "not a list"
        result = data_cleaner.clean_data(raw, "05-29-2025", "daily")
        self.assertEqual(result, 0)


if __name__ == "__main__":
    unittest.main()
