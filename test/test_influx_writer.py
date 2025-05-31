import unittest
from unittest.mock import patch, MagicMock, mock_open
from src.pipeline import influx_writer

class TestInfluxWriter(unittest.TestCase):
    @patch('src.pipeline.influx_writer.os.path.exists', return_value=False)
    @patch('builtins.open', new_callable=mock_open)
    def test_write_points_to_txt_new_file(self, mock_file, mock_exists):
        """Test write_points_to_txt creates a new file and writes data when file does not exist."""
        influx_writer.write_points_to_txt('line1\n', date_str_file='20250529', mode='daily', filename='testfile.txt')
        mock_file.assert_called_with('testfile.txt', 'w', encoding='utf-8')

    @patch('src.pipeline.influx_writer.os.path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open)
    def test_write_points_to_txt_append(self, mock_file, mock_exists):
        """Test write_points_to_txt appends data to file if it already exists and mode is not 'live'."""
        influx_writer.write_points_to_txt('line2\n', date_str_file='20250529', mode='daily', filename='testfile.txt')
        mock_file.assert_called_with('testfile.txt', 'a', encoding='utf-8')

    @patch('src.pipeline.influx_writer.os.getenv', return_value='token')
    @patch('src.pipeline.influx_writer.DB_CONFIG', new={'INFLUXDB': {'HOST': 'host', 'ORG': 'org', 'BUCKET': 'bucket'}, 'timezone': 'UTC'})
    @patch('src.pipeline.influx_writer.InfluxDBClient3')
    @patch('builtins.open', new_callable=mock_open, read_data='line1\n')
    def test_write_to_influxdb(self, mock_file, mock_client, mock_getenv):
        """Test write_to_influxdb writes data to InfluxDB using mocked client and file."""
        args = MagicMock()
        influx_writer.write_to_influxdb('testfile.txt', args, batch_size=1)
        mock_client.assert_called()

    @patch('src.pipeline.influx_writer.build_points', return_value='line1\n')
    @patch('src.pipeline.influx_writer.write_points_to_txt')
    def test_process_and_write(self, mock_write, mock_build):
        """Test process_and_write processes cleaned data and writes to file, returns correct count."""
        args = MagicMock()
        args.db_write = False
        args.override = True
        args.retain_file = False
        cleaned_list = [{'Timelogged': '05/29/2025 12:00:00 AM'}]
        count, file_path, time_str = influx_writer.process_and_write(cleaned_list, '20250529', mode='daily', args=args)
        self.assertEqual(count, 1)

    def test_should_write_point(self):
        """Test should_write_point always returns True (stub logic)."""
        args = MagicMock()
        args.override = True
        self.assertTrue(influx_writer.should_write_point({'a': 1}, args))

if __name__ == '__main__':
    unittest.main()
