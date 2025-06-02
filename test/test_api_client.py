import argparse
import datetime
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src"))
)
from src.pipeline import api_client


class TestApiClient(unittest.TestCase):
    @patch("src.pipeline.api_client.requests.get")
    @patch("src.pipeline.api_client.ElementTree.fromstring")
    def test_fetch_api_data_success(self, mock_et, mock_get):
        """Test fetch_api_data returns correct data on successful API call and XML parsing."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<string>DATA</string>"
        mock_get.return_value = mock_response
        mock_et.return_value.text = "DATA"
        result = api_client.fetch_api_data("05-29-2025", 1)
        self.assertEqual(result, "DATA")

    @patch("src.pipeline.api_client.requests.get")
    def test_fetch_api_data_http_error(self, mock_get):
        """Test fetch_api_data raises an Exception on HTTP error from API."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("HTTP error")
        mock_get.return_value = mock_response
        with self.assertRaises(Exception):
            api_client.fetch_api_data("05-29-2025", 1, max_retries=1)

    @patch("src.pipeline.api_client.requests.get")
    @patch("src.pipeline.api_client.ElementTree.fromstring")
    def test_fetch_api_data_empty(self, mock_et, mock_get):
        """Test fetch_api_data raises AssertionError if API returns empty response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<string></string>"
        mock_get.return_value = mock_response
        mock_et.return_value.text = None
        with self.assertRaises(AssertionError):
            api_client.fetch_api_data("05-29-2025", 1, max_retries=1)

    @patch("src.pipeline.api_client.requests.get")
    @patch("src.pipeline.api_client.ElementTree.fromstring")
    def test_fetch_api_data_live_success(self, mock_et, mock_get):
        """Test fetch_api_data_live returns correct data on successful live API call."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<string>LIVEDATA</string>"
        mock_get.return_value = mock_response
        mock_et.return_value.text = "LIVEDATA"
        result = api_client.fetch_api_data_live()
        self.assertEqual(result, "LIVEDATA")

    @patch("src.pipeline.api_client.fetch_api_data")
    @patch("src.pipeline.api_client.clean_and_parse_data")
    @patch("src.pipeline.api_client.process_and_write")
    @patch("src.pipeline.api_client.log_run")
    def test_process_datewise_success(
        self, mock_log_run, mock_process_and_write, mock_clean, mock_fetch
    ):
        """Test process_datewise runs full pipeline and logs run on success."""
        mock_fetch.return_value = "RAW"
        mock_clean.return_value = [{"Timelogged": "05/29/2025 12:00:00 AM"}]
        mock_process_and_write.return_value = (1, "file.txt", None)
        dt = datetime.date(2025, 5, 29)
        api_client.process_datewise(
            dt, 1, True, args=argparse.Namespace(mode="daily"), log_path="log.txt"
        )
        mock_log_run.assert_called()


if __name__ == "__main__":
    unittest.main()
