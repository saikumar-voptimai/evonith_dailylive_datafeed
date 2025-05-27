import yaml
import logging
logger = logging.getLogger('pipeline')

with open('config/field_mappings.yaml', 'r', encoding='utf-8') as f:
    FIELD_MAPPINGS = yaml.safe_load(f)

TEMP_PARAMS_MAP = FIELD_MAPPINGS.get('TEMP PARAMS MAP', {})
PROCESS_PARAMS_MAP = FIELD_MAPPINGS.get('PROCESS PARAMS MAP', {})
HEATLOAD_MAP = FIELD_MAPPINGS.get('HEATLOAD MAP', {})
MISCELLANEOUS_MAP = FIELD_MAPPINGS.get('MISC MAP', {})
COOLING_WATER_MAP = FIELD_MAPPINGS.get('COOLING WATER MAP', {})
DELTA_T_MAP = FIELD_MAPPINGS.get('DELTA T MAP', {})

# Fields that should always be written as strings in InfluxDB to avoid schema conflicts
STRING_FIELDS = {
    'hot_blast_temp_spare',
}

def get_measurement_and_field(raw_key: str):
    """
    Get the measurement and field name for a given raw key.
    Returns a tuple (measurement, field) or (None, None) if not found.
    """
    if raw_key in TEMP_PARAMS_MAP:
        return ("temperature_profile", TEMP_PARAMS_MAP[raw_key])
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

def get_numeric(value):
    """
    Convert a value to a numeric type (int or float).
    Returns None if the value is empty or cannot be converted.
    """
    try:
        if isinstance(value, str) and value.strip() == '':
            return None
        return float(value)
    except (ValueError, TypeError):
        return None

def build_points(api_dict: dict, ts) -> str:
    """
    Turn a JSON payload into one or many points, each in the correct measurement/table.
    Returns a list of influxdb_client.Point objects, one per measurement/variable.
    """
    logger.debug(f"Building points for timestamp: {ts}")
    logger.debug(f"Total {len(api_dict)} variables observed.")

    atleast_once_logged = {}
    measurement_lines = {}
    for k, v in api_dict.items():
        wrote_str, wrote_float = 0, 0
        measurement, field_info = get_measurement_and_field(k)
        if measurement is None or field_info is None:
            continue  # skip unknowns
        if field_info in STRING_FIELDS:
            v = str(v)
            continue
        if measurement not in list(atleast_once_logged.keys()):
            val = get_numeric(v)
            wrote_float += 1 if isinstance(val, (int, float)) else 0
            if val is not None:
                atleast_once_logged[measurement] = True
                measurement_lines[measurement] = f"{measurement} {field_info}={val}" 
        else:
            val = get_numeric(v)
            wrote_float += 1 if isinstance(val, (int, float)) else 0
            if val is not None:
                measurement_lines[measurement] += f",{field_info}={val}" 
    for key, _ in measurement_lines.items():
        measurement_lines[key] += f" {int(ts.timestamp())}"
    
    line_input = ''
    for measurement_line in measurement_lines.values():
        line_input += measurement_line + f"\n"
    logger.debug(f"Processeed {len(measurement_lines)} measurements, wrote_str={wrote_str}, wrote_float={wrote_float}. Total vars: {len(api_dict)}")
    return line_input
