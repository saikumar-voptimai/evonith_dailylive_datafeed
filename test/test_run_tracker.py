import unittest
from unittest.mock import patch, MagicMock
import os
from src.pipeline import run_tracker

class TestRunTracker(unittest.TestCase):
    @patch('src.pipeline.run_tracker.sqlite3.connect')
    def test_init_db(self, mock_connect):
        """Test init_db creates the database and runs table if not exists, using sqlite3 mock."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        run_tracker.init_db('db/testdb.db')
        mock_connect.assert_called_with('db/testdb.db')
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called()
        mock_conn.close.assert_called()

    @patch('src.pipeline.run_tracker.sqlite3.connect')
    def test_log_run(self, mock_connect):
        """Test log_run inserts or updates a run record in the database using sqlite3 mock."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        run_tracker.log_run('now', '20250529', '1', 'daily', {'a': 1}, 123, 1, 10, 'log.txt', 'points.txt', db_path='testdb.db')
        mock_connect.assert_called_with('testdb.db')
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called()
        mock_conn.close.assert_called()

if __name__ == '__main__':
    unittest.main()
