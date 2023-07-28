## Airtable Events ETL


### Pipeline explanation
1. Firstly, it extracts App and Web events from Airtable API to so-called 'raw zone' in S3 (subfolders `nikolay/raw-events/dt={YYYY-MM-DD}`), where non-processed json files stored in the same format they received from API.
2. Then, it transforms/concatenates json files from raw zone and store it in 'processed zone' (`nikolay/processed-events/dt={YYYY-MM-DD}`), in parquet format.
3. Loads it into Redshift DWH table `event_inc` using `COPY FROM ... FORMAT PARQUET`. Table `event_inc` (events increment) stores events needed for one current/last DAG run, when the next DAG run starts, this table is first cleared and only after that the new increment is loaded.
4. Performs simple checks on `event_inc` table.
5. Loads increment to `event` table using select from `event_inc`.
6. Performs simple checks on `event` table.
7. Loads events to `event_sequence` (SQL query can be found in `/plugins/helpers/sql.py insert_events_to_event_sequence`)

### Installation steps
1. Install docker and docker-compose.
2. Build image and run containers:
	`docker-compose up --build -d`
3. Create necessary Airflow Connections (S3 and Redshift):
	a. Go to http://localhost:8080/
	b. Login using airflow/airflow as log/pass
	c. In web UI - Admin -> Connections -> `+`
	d. Add S3 connection
	e. Add Redshift connection
4. Unpause DAG on the main page.
