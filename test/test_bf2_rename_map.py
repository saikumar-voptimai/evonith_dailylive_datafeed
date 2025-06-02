import datetime
import unittest

from src.pipeline import bf2_rename_map


class TestBf2RenameMap(unittest.TestCase):
    def test_get_measurement_and_field_found(self):
        """Test get_measurement_and_field returns correct measurement and field for known keys in all mapping dicts."""
        # Use a key from one of the maps
        for mapping, name in [
            (bf2_rename_map.TEMP_PARAMS_MAP, "temperature_profile"),
            (bf2_rename_map.PROCESS_PARAMS_MAP, "process_params"),
            (bf2_rename_map.HEATLOAD_MAP, "heatload_delta_t"),
            (bf2_rename_map.MISCELLANEOUS_MAP, "miscellaneous"),
            (bf2_rename_map.COOLING_WATER_MAP, "cooling_water"),
            (bf2_rename_map.DELTA_T_MAP, "delta_t"),
        ]:
            if mapping:
                k = next(iter(mapping))
                measurement, field = bf2_rename_map.get_measurement_and_field(k)
                self.assertEqual(measurement, name)
                self.assertEqual(field, mapping[k])

    def test_get_measurement_and_field_not_found(self):
        """Test get_measurement_and_field returns (None, None) for unknown key."""
        measurement, field = bf2_rename_map.get_measurement_and_field("not_a_key")
        self.assertIsNone(measurement)
        self.assertIsNone(field)

    def test_get_numeric(self):
        """Test get_numeric returns correct float or None for various input types and values."""
        self.assertEqual(bf2_rename_map.get_numeric("123"), 123.0)
        self.assertEqual(bf2_rename_map.get_numeric(""), None)
        self.assertEqual(bf2_rename_map.get_numeric(None), None)
        self.assertEqual(bf2_rename_map.get_numeric("abc"), None)
        self.assertEqual(bf2_rename_map.get_numeric(42), 42.0)

    def test_build_points(self):
        """Test build_points returns a string (line protocol) for a valid record and timestamp."""
        # Use a key from one of the maps
        for mapping in [
            bf2_rename_map.TEMP_PARAMS_MAP,
            bf2_rename_map.PROCESS_PARAMS_MAP,
        ]:
            if mapping:
                k = next(iter(mapping))
                record = {k: 100}
                ts = datetime.datetime.now()
                result = bf2_rename_map.build_points(record, ts)
                self.assertIsInstance(result, str)


if __name__ == "__main__":
    unittest.main()
