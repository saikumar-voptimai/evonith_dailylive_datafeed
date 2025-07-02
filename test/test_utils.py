import unittest

from src.pipeline import utils


class TestUtils(unittest.TestCase):
    def test_daterange(self):
        """Test daterange yields all dates between start and end date inclusive in correct format."""
        dates = list(utils.daterange("05-27-2025", "05-29-2025"))
        self.assertEqual(dates, ["05-27-2025", "05-28-2025", "05-29-2025"])

    def test_check_existing_data(self):
        """Test check_existing_data always returns False (stub logic)."""
        self.assertFalse(utils.check_existing_data([{"a": 1}]))

    def test_setup_run_logging_yaml(self):
        """Test setup_run_logging_yaml returns a log file path containing the log_dir and ending with .log."""
        # This will create a log file path, but we don't check file creation here
        log_path = utils.setup_run_logging_yaml(
            "05-29-2025",
            time_str="120000",
            mode="live",
            range_param="1",
            pid=12345,
            log_dir="test_logs",
            yaml_path="config/logging.yaml",
        )
        self.assertIn("test_logs", log_path)
        self.assertTrue(log_path.endswith(".log"))


if __name__ == "__main__":
    unittest.main()
