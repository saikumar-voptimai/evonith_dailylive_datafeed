import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
import sys
import os
import argparse
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import src.main

class TestMain(unittest.TestCase):
    @patch('src.main.init_db')
    @patch('src.main.setup_logging')
    @patch('src.main.fetch_api_data_live')
    @patch('src.main.clean_data')
    @patch('src.main.process_and_write')
    @patch('src.main.log_run')
    @patch('src.main.setup_run_logging_yaml', return_value='logfile.log')
    @patch('src.main.load_dotenv')
    @patch('src.main.argparse.ArgumentParser.parse_args')
    def test_main_live_mode(self, mock_args, mock_dotenv, mock_log_yaml, mock_log_run, mock_process_and_write, mock_clean_data, mock_fetch_live, mock_setup_logging, mock_init_db):
        """Test main() in live mode: should fetch, clean, process data and log run using mocks."""
        args = argparse.Namespace(
            mode='live', date=None, startdate=None, enddate=None, range=None, db_write=False, override=True, retain_file=False, debug=False, delay=120, use_db_params=False, db_host=None, db_org=None, log_run=False
        )
        mock_args.return_value = args
        mock_fetch_live.return_value = 'RAW'
        mock_clean_data.return_value = [{'Timelogged': '05/29/2025 12:00:00 AM'}]
        mock_process_and_write.return_value = (1, 'file.txt', '120000')
        with patch('src.main.CONFIG', {'DATE_FORMAT_FILENAME': '%m_%d_%Y', 'TIME_FORMAT_FILENAME': '%H_%M_%S', 'WAIT': 10}):
            src.main.main()
        mock_fetch_live.assert_called()
        mock_clean_data.assert_called()
        mock_process_and_write.assert_called()

    @patch('src.main.init_db')
    @patch('src.main.setup_logging')
    @patch('src.main.process_datewise')
    @patch('src.main.setup_run_logging_yaml', return_value='logfile.log')
    @patch('src.main.load_dotenv')
    @patch('src.main.argparse.ArgumentParser.parse_args')
    def test_main_daily_mode(self, mock_args, mock_dotenv, mock_log_yaml, mock_process_datewise, mock_setup_logging, mock_init_db):
        """Test main() in daily mode: should process a single date using process_datewise mock."""
        args = argparse.Namespace(
            mode='daily', date='05-29-2025', startdate=None, enddate=None, range=None, db_write=False, override=True, retain_file=False, debug=False, delay=120, use_db_params=False, db_host=None, db_org=None, log_run=False
        )
        mock_args.return_value = args
        with patch('src.main.CONFIG', {'DATE_FORMAT_FILENAME': '%m_%d_%Y', 'TIME_FORMAT_FILENAME': '%H_%M_%S', 'WAIT': 10}):
            src.main.main()
        mock_process_datewise.assert_called()

    @patch('src.main.init_db')
    @patch('src.main.setup_logging')
    @patch('src.main.process_datewise')
    @patch('src.main.setup_run_logging_yaml', return_value='logfile.log')
    @patch('src.main.load_dotenv')
    @patch('src.main.argparse.ArgumentParser.parse_args')
    def test_main_range_mode(self, mock_args, mock_dotenv, mock_log_yaml, mock_process_datewise, mock_setup_logging, mock_init_db):
        """Test main() in range mode: should process a date range using process_datewise mock."""
        args = argparse.Namespace(
            mode='daily', date=None, startdate='05-28-2025', enddate='05-29-2025', range=None, db_write=False, override=True, retain_file=False, debug=False, delay=120, use_db_params=False, db_host=None, db_org=None, log_run=False
        )
        mock_args.return_value = args
        with patch('src.main.CONFIG', {'DATE_FORMAT_FILENAME': '%m_%d_%Y', 'TIME_FORMAT_FILENAME': '%H_%M_%S', 'WAIT': 10}):
            src.main.main()
        mock_process_datewise.assert_called()

    @patch('src.main.init_db')
    @patch('src.main.setup_logging')
    @patch('src.main.process_datewise')
    @patch('src.main.setup_run_logging_yaml', return_value='logfile.log')
    @patch('src.main.load_dotenv')
    @patch('src.main.argparse.ArgumentParser.parse_args')
    def test_main_missing_date(self, mock_args, mock_dotenv, mock_log_yaml, mock_process_datewise, mock_setup_logging, mock_init_db):
        """Test main() in daily mode with missing date: should assign yesterday's date and process it."""
        import argparse
        from datetime import datetime, timedelta
        # Simulate no date/startdate/enddate passed
        args = argparse.Namespace(
            mode='daily',
            date=None,
            startdate=None,
            enddate=None,
            db_write=False,
            override=True,
            retain_file=False,
            debug=False,
            delay=120,
            use_db_params=False,
            db_host=None,
            db_org=None,
            log_run=False
        )
        mock_args.return_value = args
        # Patch CONFIG to have correct date format
        with patch('src.main.CONFIG', {'DATE_FORMAT_FILENAME': '%m_%d_%Y', 'TIME_FORMAT_FILENAME': '%H_%M_%S', 'WAIT': 10}):
            src.main.main()
        # Should call process_datewise with yesterday's date - when no date is specified
        called_args = mock_process_datewise.call_args[0][0]
        expected_yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().strftime("%m_%d_%Y")
        expected_yesterday_dt = datetime.strptime(expected_yesterday, "%m_%d_%Y")
        assert called_args == expected_yesterday_dt, f"Expected {expected_yesterday_dt}, got {called_args}"

if __name__ == '__main__':
    unittest.main()
