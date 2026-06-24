## Evonith Daily and Live Data Loader

This Python-based CLI tool ingests blast-furnace process variables into InfluxDB, supporting **daily**, **range backfill**, and **live** modes. It is designed for robust, automated data loading and optional local export for auditing or downstream workflows.

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
- **Range (backfill)**:  
  Manually triggered for a date range (via `--startdate` and `--enddate`).
---

### Code Organization
  - `field_mappings.yaml`: Field name mappings for flexible schema evolution.  
  - `logging.yaml`, `logging_debug.yaml`: Logging configuration.

- **src/main.py**:  
  CLI entrypoint. Parses arguments and orchestrates the fetch → transform → write pipeline.

- **src/pipeline/**:  
  - `api_client.py`: HTTP wrappers for daily (range) and live API calls.  
  - `data_cleaner.py`: Cleans and parses raw JSON, handles timestamps and missing/null fields.  
  - `bf2_rename_map.py`: Applies field mappings and builds InfluxDB point objects.  
  - `influx_writer.py`: Writes points to InfluxDB, supporting override and conditional writes.  
  - `utils.py`: Helpers (e.g., `daterange` generator).

- **logs/**: Runtime logs (per run/date).
- **output/**: Sample exports and file dumps.
- **test/**: Unit test scaffolding.

---

### Business Logic

- Loads blast-furnace process variables in three modes (Daily, Range backfill, Live).
- Optionally dumps raw points to a local gzipped file for auditing or downstream workflows.

---

### Technical Details

#### 1. Data Flow in `main.py`

1. **Parse CLI flags**:  
  - Mode, dates, DB write/override, file retention, delays, host/org overrides, optional variable filtering.
2. **Load environment**:  
   - `.env` and logging config.
3. **Live mode**:  
  - One run: `fetch_api_data_live()` → process → optional DB/file write → sleep to maintain cadence.
4. **Daily/Range mode**:  
  - Daily: processes a single date (defaults to yesterday when `--date` omitted).
  - Range: processes a date list derived from `--startdate` and `--enddate`.
5. **process_and_write()**:  
  a. `clean_and_parse_data(raw)` → list of records  
  b. `build_points(record, timestamp)` → InfluxDB line protocol  
  c. If `--db-write` is enabled, writes to InfluxDB (currently only when `--override` is enabled)  
  d. Always stages line protocol in a temp file; if `--retain-file` is enabled, it is gzipped and kept.

#### 2. Configuration and Extensibility

- **YAML** and **.env** files drive endpoints, credentials, bucket names, logging levels, and timing.
- CLI flags allow toggling DB writes, override behavior, live vs. historic ingestion, and file exports.
- Field mappings can be updated without code changes via `field_mappings.yaml`.

---

### Outstanding TODOs - This is being done as separate batch job

- **influx_writer.py**:  
  Implement existence check to avoid duplicate writes.
- **bf2_rename_map.py**:  
  Revisit handling of string/empty fields—convert to NULL where appropriate.

---

### CLI Usage

The main entrypoint is `main.py`, which supports the following arguments:

- `--mode` (required):
  - `daily` — Loads data for a single date (defaults to yesterday when `--date` omitted)
  - `range` — Loads data for a date range using `--startdate` and `--enddate`
  - `live` — Fetches and loads real-time data
- `--date` (optional):
  - Date in `MM-DD-YYYY` format (used in daily mode).
- `--startdate` and `--enddate` (optional):
  - Date range in `MM-DD-YYYY` format (used in range mode).
- `--db-write` (boolean flag, optional):
  - Enable DB writes. Supports `--db-write`, `--db-write True/False`, and `--no-db-write`.
- `--override` (flag, optional):
  - If set, overwrites existing points in InfluxDB.
- `--retain-file` (boolean flag, optional):
  - If set, retains a gzipped points file in `output/`.
  - If not set, the temp staging file `output/tmp_<pid>.txt` is removed at the end of the run.
- `--delay` (optional):
  - Delay in seconds between API calls (also used as a fallback cadence for live).
- `--host` and `--org` (optional):
  - Override InfluxDB host and organization from config.
- `--debug` (boolean flag, optional):
  - Enables debug logging config.
- `--log-run` (boolean flag, optional):
  - Records run metadata in a local SQLite DB (`db/run_metadata.db`) and creates per-run log files.
- `--variable-file` (optional):
  - Path to a `.txt` file containing variable names (one per line) to limit which fields are written (used during range backfill).

Example usage:

```sh
uv run python src/main.py --mode daily --db-write --retain-file
uv run python src/main.py --mode range --startdate 01-01-2025 --enddate 01-10-2025 --db-write --variable-file variables.txt
uv run python src/main.py --mode live --db-write --no-retain-file
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
  uv run python src/main.py --mode daily --db-write
   ```

- All CLI arguments can be combined as needed.
- Ensure your config files and environment variables are set up as described above.

### Tests

Run unit tests with:

```sh
python -m pytest -q
```

If `pytest` is not installed in your environment, install it with:

```sh
pip install pytest
```
