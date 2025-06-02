import argparse
import datetime
import unittest
from unittest.mock import MagicMock, patch

from src.pipeline import (api_client, bf2_rename_map, data_cleaner,
                          influx_writer)


class TestFullPipeline(unittest.TestCase):
    @patch("src.pipeline.api_client.requests.get")
    @patch("src.pipeline.api_client.ElementTree.fromstring")
    def test_fetch_and_clean_daily(self, mock_et, mock_get):
        """Integration: fetch_api_data returns XML, clean_and_parse_data parses to records."""
        # Simulate API XML response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = (
            '<string>[{"Timelogged": "05/29/2025 12:00:00 AM", "var1": 123}]</string>'
        )
        mock_get.return_value = mock_response
        mock_et.return_value.text = (
            '[{"Timelogged": "05/29/2025 12:00:00 AM", "var1": 123}]'
        )
        raw = api_client.fetch_api_data("05-29-2025", 1)
        records = data_cleaner.clean_and_parse_data(raw)
        self.assertIsInstance(records, list)
        self.assertEqual(records[0]["var1"], 123)

    @patch("src.pipeline.influx_writer.write_points_to_txt")
    @patch(
        "src.pipeline.bf2_rename_map.build_points",
        return_value="measurement field=1 1234567890\n",
    )
    def test_process_and_write_integration(
        self, mock_build_points, mock_write_points_to_txt
    ):
        """Integration: process_and_write processes cleaned data and writes to file."""
        cleaned_list = [{"Timelogged": "05/29/2025 12:00:00 AM", "var1": 123}]
        args = MagicMock()
        args.db_write = False
        args.override = True
        args.retain_file = False
        count, file_path, time_str = influx_writer.process_and_write(
            cleaned_list, "20250529", mode="daily", args=args
        )
        self.assertEqual(count, 1)
        mock_write_points_to_txt.assert_called()

    @patch("src.pipeline.api_client.fetch_api_data")
    @patch("src.pipeline.data_cleaner.clean_and_parse_data")
    @patch("src.pipeline.influx_writer.process_and_write")
    @patch("src.pipeline.api_client.log_run")
    def test_process_datewise_pipeline(
        self, mock_log_run, mock_process_and_write, mock_clean, mock_fetch
    ):
        """Integration: process_datewise runs the full pipeline and logs run."""
        mock_fetch.return_value = (
            '[{"Timelogged": "05/29/2025 12:00:00 AM", "var1": 123}]'
        )
        mock_clean.return_value = [
            {"Timelogged": "05/29/2025 12:00:00 AM", "var1": 123}
        ]
        mock_process_and_write.return_value = (1, "file.txt", None)
        dt = datetime.date(2025, 5, 29)
        args = argparse.Namespace(
            mode="daily",
            db_write="False",
            log_run="True",
            override="True",
            retain_file="False",
            debug="False",
            delay=120,
            use_db_params="False",
            db_host=None,
            db_org=None,
        )
        api_client.process_datewise(dt, 1, True, args=args, log_path="log.txt")
        mock_log_run.assert_called()


if __name__ == "__main__":
    unittest.main()
