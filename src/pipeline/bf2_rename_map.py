import re
import yaml
from typing import Optional, Tuple
from influxdb_client_3 import Point
import logging
logger = logging.getLogger('pipeline.bf2_rename_map')

with open('config/field_mappings.yaml', 'r', encoding='utf-8') as f:
    FIELD_MAPPINGS = yaml.safe_load(f)

PROCESS_PARAMS_MAP = FIELD_MAPPINGS.get('PROCESS PARAMS MAP', {})
HEATLOAD_MAP = FIELD_MAPPINGS.get('HEATLOAD MAP', {})
MISCELLANEOUS_MAP = FIELD_MAPPINGS.get('MISC MAP', {})
COOLING_WATER_MAP = FIELD_MAPPINGS.get('COOLING WATER MAP', {})
DELTA_T_MAP = FIELD_MAPPINGS.get('DELTA T MAP', {})

# Temperature variable handling
TEMP_REGEX = re.compile(r"BF2_BFBD Furnace Body (?P<level>\d+)mm Temp (?P<tc>[A-N])", re.I)
ALLOWED_TEMP_VARS = set([
    # Add all allowed temperature variable names here or load from config if needed
])

# Fields that should always be written as strings in InfluxDB to avoid schema conflicts
STRING_FIELDS = {
    'hot_blast_temp_spare',
    # add other field names here as needed
}

def parse_furnace_temp(raw_key: str) -> Optional[Tuple[str, str]]:
    m = TEMP_REGEX.fullmatch(raw_key.strip())
    if not m:
        return None
    return m.group("level"), m.group("tc").lower()

def get_measurement_and_field(raw_key: str):
    # Temperature profile
    if TEMP_REGEX.fullmatch(raw_key.strip()):
        parsed = parse_furnace_temp(raw_key)
        if parsed:
            return ("temperature_profile", parsed)
    if raw_key in PROCESS_PARAMS_MAP:
        return ("process_params", PROCESS_PARAMS_MAP[raw_key])
    if raw_key in HEATLOAD_MAP:
        return ("heatload_delta_t", HEATLOAD_MAP[raw_key])
    if raw_key in MISCELLANEOUS_MAP:
        return ("miscellaneous", MISCELLANEOUS_MAP[raw_key])
    if raw_key in COOLING_WATER_MAP:
        return ("cooling_water", COOLING_WATER_MAP[raw_key])
    if raw_key in DELTA_T_MAP:
        return ("delta_t", DELTA_T_MAP[raw_key])
    return (None, None)

def build_points(api_dict: dict, ts) -> list[Point]:
    """
    Turn a JSON payload into one or many points, each in the correct measurement/table.
    Returns a list of influxdb_client.Point objects, one per measurement/variable.
    """
    points = []
    logger.info(f"Building points for timestamp: {ts}")
    logger.info(f"Total {len(api_dict)} variables observed.")
    
    wrote_floats, wrote_strs, wrote_temps = 0, 0, 0
    for k, v in api_dict.items():
        measurement, field_info = get_measurement_and_field(k)
        if measurement is None or field_info is None:
            continue  # skip unknowns
        if measurement == "temperature_profile":
            # field_info: (level, tc)
            level, tc = field_info
            try:
                pt = Point(measurement).time(ts)
                pt = pt.tag("level_mm", level).tag("thermocouple", tc).field("temp_c", float(v))
                points.append(pt)
                wrote_temps += 1
            except Exception:
                continue
        else:
            # If this field must be a string, cast and write directly
            # TODO: Revisit to handle strings or empty strings in Fields -> Convert to NULL
            if field_info in STRING_FIELDS:
                try:
                    pt = Point(measurement).time(ts)
                    pt = pt.field(field_info, str(v))
                    points.append(pt)
                except Exception:
                    logger.warning(f"Failed to write string field {measurement}.{field_info}: {v}")
                continue
            # Otherwise, attempt float; empty string as NULL
            try:
                pt = Point(measurement).time(ts)
                pt = pt.field(field_info, float(v))
                points.append(pt)
                wrote_floats += 1
            except (ValueError, TypeError):
                if v == '' or v is None:
                    try:
                        pt = Point(measurement).time(ts)
                        pt = pt.field(field_info, None)
                        points.append(pt)
                        wrote_strs += 1
                    except Exception:
                        continue
                else:
                    logger.warning(f"Skipping non-numeric {measurement}.{field_info}: {v}")
                continue
    logger.info(f"Wrote {wrote_floats} float points, {wrote_strs} string points, and {wrote_temps} \
                temperature points. Total points: {wrote_strs + wrote_floats + wrote_temps}")
    return points
