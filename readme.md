## Evonith Daily and Live Data Loader

This Python-based CLI tool ingests blast-furnace process variables into InfluxDB, supporting daily, historic, and live modes. It is designed for robust, automated data loading, downsampling, and optional local export for auditing or downstream workflows.

---

### Modes of Operation

- **Daily**:  
  Runs as a cronjob at 12:30 AM, fetching data for the previous day and writing to the `bucket_raw` InfluxDB bucket.  
  Example: On April 3rd, fetches data for April 2nd.

- **Historic**:  
  Manually triggered for a specific date or date range (via CLI flags).

- **Live**:  
  Runs as a cronjob every 10 seconds, fetching real-time data from the live API and writing to InfluxDB.

- **Downsampling**:  
  In Daily/Historic modes, computes 5-minute averages and writes to a dedicated `bucket_5min`.

---

### Code Organization

- **config/**:  
  - `config.yaml`: API endpoints, credentials, bucket names, timing, etc.  
  - `field_mappings.yaml`: Field name mappings for flexible schema evolution.  
  - `logging.yaml`, `logging_debug.yaml`: Logging configuration.

- **src/main.py**:  
  CLI entrypoint. Parses arguments and orchestrates the fetch → transform → write pipeline.

- **src/pipeline/**:  
  - `api_client.py`: HTTP wrappers for historic and live API calls.  
  - `data_cleaner.py`: Cleans and parses raw JSON, handles timestamps and missing/null fields.  
  - `bf2_rename_map.py`: Applies field mappings and builds InfluxDB point objects.  
  - `influx_writer.py`: Writes points to InfluxDB, supporting override and conditional writes.  
  - `write_to_file.py`: Serializes and writes points to text/CSV files by date and mode.  
  - `utils.py`: Helpers (e.g., `daterange` generator).

- **logs/**: Runtime logs (per run/date).
- **output/**: Sample exports and file dumps.
- **test/**: Unit test scaffolding.

---

### Business Logic

- Loads blast-furnace process variables in three modes (Daily, Historic, Live).
- In Daily/Historic modes, performs 5-minute downsampling and writes to a dedicated bucket.
- Optionally dumps all raw points to text/CSV for auditing or downstream workflows.

---

### Technical Details

#### 1. Data Flow in `main.py`

1. **Parse CLI flags**:  
   - Mode, dates, DB write/override, file-dump, delays, host/org overrides.
2. **Load environment**:  
   - `.env` and logging config.
3. **Live mode**:  
   - Loop: `fetch_api_data_live()` → process → optional DB/file write → sleep.
4. **Daily/Historic mode**:  
   - For each date: `fetch_api_data(date)` → process → optional delay.
5. **process_and_write()**:  
   a. `clean_and_parse_data(raw)` → list of records  
   b. `build_points(record, timestamp)` → list of InfluxDB points  
   c. Conditionally call `write_to_influxdb()` (override or `should_write_point()`)  
   d. If enabled, call `write_points_to_txt()` to dump all points.

#### 2. Configuration and Extensibility

- **YAML** and **.env** files drive endpoints, credentials, bucket names, logging levels, and timing.
- CLI flags allow toggling DB writes, override behavior, live vs. historic ingestion, and file exports.
- Field mappings can be updated without code changes via `field_mappings.yaml`.

---

### Outstanding TODOs

- **influx_writer.py**:  
  Implement existence check to avoid duplicate writes.
- **bf2_rename_map.py**:  
  Revisit handling of string/empty fields—convert to NULL where appropriate.

---

### CLI Usage

The main entrypoint is `main.py`, which supports the following arguments:

- `--mode` (required):
  - `daily` — Fetches and loads data for the previous day (default for scheduled runs)
  - `historic` — Loads data for a specific date or date range
  - `live` — Continuously fetches and loads real-time data
- `--date` (optional):
  - Date in `YYYY-MM-DD` format. Used in historic mode for a single day.
- `--start-date` and `--end-date` (optional):
  - Date range in `YYYY-MM-DD` format. Used in historic mode for multiple days.
- `--write-db` (flag, optional):
  - If set, writes data to InfluxDB. If omitted, no DB write occurs.
- `--override` (flag, optional):
  - If set, overwrites existing points in InfluxDB.
- `--write-file` (flag, optional):
  - If set, writes all points to a local file (txt/CSV) for auditing.
- `--delay` (optional):
  - Delay in seconds between processing each day (useful for rate limiting in historic mode).
- `--host` and `--org` (optional):
  - Override InfluxDB host and organization from config.
- `--config` (optional):
  - Path to a custom config YAML file.
- `--log-level` (optional):
  - Set logging level (e.g., DEBUG, INFO, WARNING).

Example usage:

```sh
uv run python main.py --mode daily --write-db --write-file
uv run python main.py --mode historic --start-date 2025-01-01 --end-date 2025-01-10 --write-db
uv run python main.py --mode live --write-db
```

---

### Running with uv

1. Install [uv](https://github.com/astral-sh/uv) if not already installed:
   ```sh
   pip install uv
   ```
2. Install dependencies:
   ```sh
   uv pip install -r requirements.txt
   # or, if using pyproject.toml:
   uv pip install -r pyproject.toml
   ```
3. Run the CLI:
   ```sh
   uv run python main.py --mode daily --write-db
   ```

- All CLI arguments can be combined as needed.
- Ensure your config files and environment variables are set up as described above.
