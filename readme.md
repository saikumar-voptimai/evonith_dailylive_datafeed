## Evonith Daily and Live data loader
I am scripting this python based code that runs using CLI and hence the argument parser.

Like in my current role, we need a service that runs every night to load the data to any
timeseries/other db and here we have chosen InfluxDB.

The current script runs in three modes -

Daily - A cronjob to run the daily api and fetch data every day at 12.30AM, loads data for previous day, write it to influxDB.
For example, on 03rd of April, it fetches the data for 02nd of April and flush writes it to the InfluxDB bucket_raw Bucket.
Cron is configured on the VM/Server.

Historic - A manually triggered job that runs for a specific date or range of dates given the startdate and enddate

Live - A cronjob to run every 10s, fetch live data from live APi, write to influxDB. 
Cron is configured on the VM/Server.

TODO:
Parallely, the job when running in Daily/Historic modes does the Downsampling, for example, takes 5min average of the data and writes it to bucket_5min.

Code organisation:

## Business Logic
- Provides a data‐loading service for blast‐furnace process variables, supporting:
  - **Daily** mode (cronjob at 12:30 AM to ingest the previous day)  
  - **Historic** mode (on‐demand by date or date range)  
  - **Live** mode (poll API every ~10 s for real‐time values)
- In Daily/Historic modes, computes 5 min downsampling and writes to a dedicated bucket.
- Optional local dump of all raw points to text/CSV for auditing or downstream workflows.

## Technical Details

### 1. Project Layout
- **config/**: YAML for API endpoints, field mappings, logging, and timing (WAIT).
- **src/main.py**: CLI entrypoint that parses arguments, orchestrates fetch → transform → write.
- **src/pipeline/**:
  - **api_client.py**: HTTP wrappers for historic vs live API calls.
  - **data_cleaner.py**: Sanitizes JSON, parses timestamps, handles null/missing fields.
  - **bf2_rename_map.py**: Applies field‐name mappings and builds InfluxDB point objects.
  - **influx_writer.py**: Writes points to InfluxDB; supports full override or conditional writes.
  - **write_to_file.py**: Serializes and writes points to text/CSV files by date and mode.
  - **utils.py**: Helpers (e.g. `daterange` generator).
- **logs/** and **output/**: Runtime logs and sample exports.
- **test/**: Scaffold for pipeline unit tests.

### 2. Data Flow in `main.py`
1. Parse CLI flags: mode, dates, DB write/override, file‐dump, delays, host/org overrides.  
2. Load environment (`.env`) and logging config.  
3. **Live**: loop → `fetch_api_data_live()` → process → optional DB/file write → sleep.  
4. **Daily/Historic**: for each date → `fetch_api_data(date)` → process → optional delay.  
5. **process_and_write()**:  
   a. `clean_and_parse_data(raw)` → list of records  
   b. `build_points(record, timestamp)` → list of InfluxDB points  
   c. Conditionally call `write_to_influxdb()` based on override or `should_write_point()`  
   d. If enabled, call `write_points_to_txt()` to dump all points.

### 3. Configuration and Extensibility
- **YAML** and **.env** drive endpoints, credentials, bucket names, logging levels, and timing.  
- CLI flags allow toggling DB writes, override behavior, live vs historic ingestion, and file exports.  
- Field mappings can be updated without code changes via `field_mappings.yaml`.

## Outstanding TODOs
- **influx_writer.py**: implement existence check to avoid duplicate writes.  
- **bf2_rename_map.py**: revisit handling of string/empty fields → convert to NULL where appropriate.
