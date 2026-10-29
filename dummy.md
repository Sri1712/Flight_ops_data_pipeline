
# ✈️ Flight Operations Data Engineering Pipeline

An end-to-end flight data engineering project built with **Apache Airflow, Python, Docker, and Snowflake**. The pipeline ingests live flight state data from the OpenSky Network API, processes it through a Medallion-style architecture (Bronze, Silver, and Gold), and loads aggregated flight activity KPIs into Snowflake for analytics and reporting.

## 📌 Project Overview

This project demonstrates how to build an automated data pipeline that collects flight state information, transforms raw API responses into structured datasets, aggregates flight activity metrics, and stores the results in a cloud data warehouse.

Apache Airflow orchestrates the workflow, while Python and Pandas handle ingestion and data transformation. Snowflake serves as the final analytical destination.

The project focuses on analyzing global flight activity patterns rather than tracking individual flights or supporting air traffic control.

## 🎯 Project Objectives

- Ingest flight state data from a real external API.
- Orchestrate data processing using Apache Airflow.
- Implement Bronze, Silver, and Gold data layers.
- Transform raw JSON responses into structured CSV datasets.
- Aggregate flight activity metrics by origin country.
- Load analytical KPIs into Snowflake.
- Use Snowflake `MERGE` logic to insert or update KPI records.
- Demonstrate task dependencies, scheduling, and failure handling.
- Prepare analytical data for downstream BI tools such as Power BI.

## 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data ingestion and processing |
| Apache Airflow | Workflow orchestration and scheduling |
| OpenSky Network API | Flight state data source |
| Pandas | Data transformation and aggregation |
| Requests | HTTP API requests |
| Docker | Containerized execution environment |
| Docker Compose | Local service orchestration |
| Snowflake | Cloud data warehouse |
| Snowflake Connector for Python | Database connectivity and loading |
| SQL | KPI upserts using `MERGE` |
| Git and GitHub | Version control |

## 🏗️ Data Pipeline Architecture

```text
            OpenSky Network API
          /api/states/all endpoint
                     |
                     v
          Apache Airflow DAG
          flight_data_pipeline
                     |
                     v
          1. BRONZE INGEST
          PythonOperator
          Raw API response as JSON
                     |
                     v
          2. SILVER TRANSFORM
          PythonOperator
          Selected fields in CSV
                     |
                     v
          3. GOLD AGGREGATE
          PythonOperator
          Country-level flight KPIs
                     |
                     v
          4. GOLD TO SNOWFLAKE
          PythonOperator
          Snowflake MERGE operation
                     |
                     v
              SNOWFLAKE
              FLIGHTS.KPI
              FLIGHT_KPIS
                     |
                     v
          Analytics / Power BI
                 (Optional)
```

## 🥉 Bronze Layer — Raw Flight Data Ingestion

**Airflow task:** `bronze_ingest`

**Function:** `run_bronze_ingest()`

The Bronze layer retrieves the current flight state dataset from the OpenSky Network API.

**API endpoint:**

`https://opensky-network.org/api/states/all`

### Processing steps

1. Sends an HTTP GET request with a 60-second timeout.
2. Checks the HTTP response for errors.
3. Parses the response as JSON.
4. Writes the complete response to a timestamped JSON file.
5. Passes the file path to the next Airflow task using XCom.

### Output

Example filename:

`data/bronze/flights_20261029_143000.json`

The exact filename depends on the ingestion timestamp.

The raw response contains API metadata and a collection of flight state vectors, preserving source information before downstream transformation.

## 🥈 Silver Layer — Data Transformation

**Airflow task:** `silver_transform`

**Function:** `run_silver_transform()`

The Silver layer reads the Bronze JSON file, converts the flight state vectors into a Pandas DataFrame, selects relevant fields, and writes the result to CSV.

### Selected fields

| Column | Description |
|---|---|
| `icao24` | Unique 24-bit aircraft transponder address |
| `origin_country` | Country of origin reported by the API |
| `velocity` | Aircraft velocity in metres per second |
| `on_ground` | Indicates whether the aircraft is on the ground |
| `geo_altitude` | Geometric altitude in metres |

### Processing steps

1. Retrieves the Bronze file path through Airflow XCom.
2. Loads the JSON response.
3. Maps the API state-vector positions to their corresponding column names.
4. Selects the five fields required for downstream analytics.
5. Saves the transformed records to CSV.
6. Publishes the Silver file path through XCom.

### Output

Example filename:

`data/silver/flights_20261029.csv`

The output filename is based on the Airflow execution date.

## 🥇 Gold Layer — Flight Activity Aggregation

**Airflow task:** `gold_aggregate`

**Function:** `run_gold_aggregate()`

The Gold layer aggregates the Silver dataset by `origin_country` to produce country-level flight activity KPIs.

### Metrics calculated

| Metric | Calculation | Description |
|---|---|---|
| `total_flights` | Count of `icao24` records | Number of flight state records per country |
| `avg_velocity` | Mean of `velocity` | Average reported velocity in metres per second |
| `on_ground` | Sum of `on_ground` | Number of records marked as being on the ground |
| `avg_geo_altitude` | Mean of `geo_altitude` | Average geometric altitude in metres |

**Note:** `total_flights` counts records in the API response, not necessarily unique aircraft or completed flights. Similarly, `on_ground` counts records marked as grounded, not individual aircraft after deduplication.

### Processing steps

1. Retrieves the Silver CSV file path through XCom.
2. Loads the dataset using Pandas.
3. Groups records by `origin_country`.
4. Calculates the four aggregate metrics.
5. Writes the aggregated results to CSV.
6. Publishes the Gold file path through XCom.

### Output

Example filename:

`data/gold/flights_aggregated_20261029.csv`

The Gold layer provides a compact dataset suitable for loading into Snowflake and querying for flight activity trends.

## ❄️ Snowflake Layer — Analytics Data Warehouse

**Airflow task:** `gold_to_snowflake`

**Function:** `gold_to_snowflake()`

Snowflake is the final destination for the aggregated Gold dataset.

### Target table

`FLIGHTS.KPI.FLIGHT_KPIS`

The loader connects to Snowflake using the Airflow connection ID:

`flight_snowflake`

Connection settings are retrieved from the Airflow connection's login, password, and extra configuration fields.

### Loading process

1. Retrieves the Gold CSV file path through XCom.
2. Loads the aggregated data into a Pandas DataFrame.
3. Normalizes column names to uppercase.
4. Converts missing Pandas values to Python `None` values where supported by the conversion.
5. Connects to Snowflake.
6. Executes a `MERGE` statement for each country-level KPI row.
7. Updates an existing KPI record or inserts a new record.

### Snowflake KPI columns

| Column | Purpose |
|---|---|
| `WINDOW_START` | Timestamp identifying the processing window |
| `ORIGIN_COUNTRY` | Country associated with the aggregate |
| `TOTAL_FLIGHTS` | Count of flight state records |
| `AVG_VELOCITY` | Average reported aircraft velocity |
| `ON_GROUND` | Count of records marked as grounded |
| `AVG_GEO_ALTITUDE` | Average geometric altitude |
| `LOAD_TIME` | Timestamp of the most recent update to a matched record |

### Incremental loading and MERGE logic

The loader matches records using:

- `WINDOW_START`
- `ORIGIN_COUNTRY`

If a matching record exists, its KPI values are updated. Otherwise, a new record is inserted.

This makes the load suitable for rerunning a processing window without inserting duplicate rows for the same key, provided the target table and its key semantics are configured consistently.

## 🔄 Airflow Orchestration

The pipeline is orchestrated through the DAG:

`flight_data_pipeline`

The DAG contains four sequential tasks:

```text
bronze_ingest
      |
      v
silver_transform
      |
      v
gold_aggregate
      |
      v
gold_to_snowflake
```

### Orchestration features

- Scheduled execution.
- Explicit task dependencies.
- XCom-based file path passing between tasks.
- Task-level execution monitoring.
- Failure visibility through Airflow logs.
- Retry behavior according to the DAG's configured retry settings.

The DAG screenshot demonstrates the four-task workflow and its execution history. The actual schedule, retry count, and retry delay are determined by the DAG configuration.

## 📁 Project Structure

```text
Flight_ops_data_pipeline/
│
├── dags/
│   └── flight_data_pipeline.py
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── logs/
├── plugins/
├── scripts/
├── sql/
│
├── .env
├── .gitignore
├── docker-compose.yml
├── requirements.txt
└── README.md
```

*The filenames above are illustrative; adjust them to match the files actually present in your repository.*

## 🚀 Getting Started

### Prerequisites

- Git
- Docker Desktop with Docker Compose
- Python compatible with the project dependencies
- Access to the OpenSky Network API
- A Snowflake account with appropriate permissions
- An Airflow environment configured to use the Snowflake connector

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/flight-ops-data-pipeline.git
cd flight-ops-data-pipeline
```

Replace `YOUR_USERNAME` with your GitHub username.

### 2. Configure the environment

Configure the environment variables and Airflow settings required by your Docker Compose configuration.

Keep local secrets in `.env` or your approved secret-management mechanism. Do not commit credentials to GitHub.

### 3. Install dependencies

Ensure the Airflow runtime has the libraries required by the tasks, including:

- `requests`
- `pandas`
- `snowflake-connector-python`

Airflow's Python environment must also be compatible with the installed package versions.

If the project uses `requirements.txt`, install the dependencies according to your Docker image and Airflow setup.

### 4. Configure the Snowflake connection in Airflow

In the Airflow UI, navigate to **Admin → Connections** and configure a connection with the ID:

`flight_snowflake`

Set the connection's login and password or other supported authentication values. Add the required Snowflake account, warehouse, database, schema, and role fields in the connection's Extra configuration as expected by the code.

Ensure that the Snowflake target table exists and the configured role has the necessary permissions.

### 5. Start the Docker environment

From the project root, run:

```bash
docker compose up -d
```

Check the service status:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f
```

### 6. Run the Airflow pipeline

1. Open the Airflow web interface using the address configured in Docker Compose.
2. Find the `flight_data_pipeline` DAG.
3. Enable the DAG if it is paused.
4. Trigger a manual run.
5. Monitor the four tasks in the Graph view.
6. Inspect task logs if a task fails.
7. Verify the generated files and the loaded KPI records in Snowflake.

## 📊 Analytics Use Cases

The Gold KPI table can support questions such as:

- How does observed flight activity vary by origin country?
- What is the average reported aircraft velocity by country?
- How many observed flight state records are marked as on the ground?
- How do aggregated metrics change across processing windows?

These metrics describe observations returned by the API at ingestion time. They should not be interpreted as a complete count of all flights operated during a time period.

## 🔐 Data Management and Security

The following files and folders should generally remain outside version control:

- `.env` and files containing credentials.
- `.venv/` and other local virtual environments.
- `__pycache__/` and compiled Python files.
- Generated logs.
- Large or sensitive datasets in `data/`.

Use `.gitignore` to exclude generated files. If reproducibility requires sample data, publish only small, non-sensitive samples.

## ⚠️ Current Implementation Notes

- The API request retrieves the current state-vector snapshot, rather than a complete historical flight-event stream.
- The Bronze and Silver layers use timestamped or execution-date-based local files.
- The Gold aggregation is performed by country for the records in the retrieved snapshot.
- The Snowflake loader uses the Airflow data interval's start timestamp as `WINDOW_START`.
- The current aggregation groups by country; it does not independently aggregate each record into multiple 30-minute event windows.
- Missing numeric values may still require additional handling to guarantee valid Snowflake numeric values.

These details are important when interpreting the data and extending the project into a more robust historical analytics pipeline.

## 🚀 Future Improvements

- Add data quality checks and validation.
- Handle API rate limits and transient request failures.
- Add explicit pagination or geographic filtering where supported and needed.
- Improve historical snapshot retention.
- Add robust handling for missing and infinite numeric values.
- Optimize Snowflake loading using batch operations.
- Add automated tests for ingestion and transformation logic.
- Implement monitoring, alerting, and data freshness checks.
- Connect Snowflake to Power BI for interactive analytics.

## 👤 Author

**Your Name**

GitHub: https://github.com/YOUR_USERNAME

## 📄 License

Add a license if you intend to distribute the project publicly.



## 📌 Project Overview

This project demonstrates a production-style batch data pipeline for monitoring global flight activity patterns. It focuses on automated ingestion, data cleaning, incremental processing, orchestration, and analytical data modeling.

Apache Airflow orchestrates the pipeline, Python handles data ingestion and transformation, and PostgreSQL stores analytics-ready flight activity metrics.

The pipeline is designed for historical and near-real-time batch analytics, rather than individual flight tracking or air traffic control.

## 🎯 Project Objectives

- Ingest live flight data from an external REST API.
- Automate pipeline execution using Apache Airflow.
- Implement Medallion Architecture (Bronze → Silver → Gold).
- Build incremental, time-windowed data processing workflows.
- Store historical flight activity KPIs in PostgreSQL.
- Implement idempotent processing and UPSERT logic.
- Configure task dependencies, retries, and failure handling.
- Prepare analytics-ready data for Power BI dashboards.

## 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data ingestion and transformation |
| Apache Airflow | Scheduling and orchestration |
| REST API | Live flight data source |
| PostgreSQL | Analytical data storage |
| Docker | Containerized services |
| Docker Compose | Local environment orchestration |
| SQL | Data transformation and KPI aggregation |
| Power BI | Optional business intelligence and visualization |
| Git and GitHub | Version control |

## 🏗️ Architecture

```text
        External Flight Data API
                   |
                   v
          Apache Airflow DAG
                   |
                   v
          BRONZE LAYER
          Raw API responses
                   |
                   v
          SILVER LAYER
      Cleaned and normalized data
                   |
                   v
           GOLD LAYER
       Aggregated flight KPIs
                   |
                   v
              PostgreSQL
                   |
                   v
          Power BI / Analytics
```

## 🥉 Bronze Layer — Raw Data

The Bronze layer stores raw flight data retrieved from the external API.

Responsibilities:
- Retrieve flight data through HTTP requests.
- Preserve raw API responses for downstream processing.
- Organize data by ingestion time or processing window.
- Support traceability and potential reprocessing.

## 🥈 Silver Layer — Cleaned Data

The Silver layer converts raw API responses into structured, consistent datasets.

Responsibilities:
- Normalize fields and data types.
- Handle missing or invalid values.
- Remove duplicate records where appropriate.
- Apply data quality checks.
- Prepare records for aggregation.

## 🥇 Gold Layer — Analytics

The Gold layer contains aggregated metrics designed for reporting and trend analysis.

Potential analytical use cases include:
- Flight activity volume over time.
- Comparisons of activity across countries.
- Historical activity trends by time window.
- Identification of potential air traffic congestion signals.

The exact KPIs depend on the fields available from the chosen API.

## 🔄 Orchestration and Reliability

Apache Airflow coordinates the workflow through DAGs and task dependencies.

The project focuses on:
- Scheduled pipeline execution.
- Configurable retry policies.
- Task dependency management.
- Idempotent, rerunnable processing.
- Incremental, time-windowed data ingestion.
- Failure monitoring and task logs.

## 🗄️ Data Storage

PostgreSQL serves as the analytical storage layer.

The pipeline uses UPSERT-style processing to insert new records and update existing records according to the chosen business key. This helps avoid duplicate KPI records when a processing window is rerun.

## 📊 Analytics and Visualization

The Gold layer is intended to support BI tools such as Power BI.

Example analyses include:
- Flight activity by country.
- Activity trends over time.
- Comparisons between time windows.
- Changes in aggregated flight volumes.

Power BI integration is optional and can be configured separately.

## 📁 Project Structure

```text
Flight_ops_data_pipeline/
├── dags/                 # Airflow DAG definitions
├── data/                 # Local Bronze, Silver and Gold data
├── logs/                 # Local execution logs
├── plugins/              # Custom Airflow plugins, if applicable
├── scripts/               # Ingestion and transformation scripts
├── sql/                   # SQL queries and transformations
├── .env.example           # Example environment configuration
├── .gitignore             # Excluded files and generated data
├── docker-compose.yml     # Docker Compose configuration
├── requirements.txt       # Python dependencies
└── README.md              # Project documentation
```

## 🚀 Getting Started

### Prerequisites

- Git
- Docker Desktop with Docker Compose
- Python version compatible with the project dependencies
- Access to the external flight data API
- API credentials, if required by the provider

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/flight-ops-data-pipeline.git
cd flight-ops-data-pipeline
```

### 2. Configure environment variables

Create a local environment file from the provided template:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Configure the API credentials, PostgreSQL connection settings, and other variables required by the project.

Never commit credentials or secrets to GitHub.

### 3. Start the services

```bash
docker compose up -d
```

Check service status:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f
```

### 4. Open Airflow

If the Docker configuration exposes the default Airflow webserver port, open:

http://localhost:8080

Use the credentials configured for your local environment.

### 5. Run the pipeline

1. Open the Airflow interface.
2. Locate the flight operations DAG.
3. Enable the DAG if necessary.
4. Trigger a run or wait for its configured schedule.
5. Monitor task execution and inspect task logs.
6. Verify the data produced by each layer.
7. Query the Gold tables in PostgreSQL.

The exact DAG name, schedule, API configuration, and database setup depend on the project implementation.

## 🧪 Data Quality and Testing

Recommended validation checks include:

- API response and schema validation.
- Handling of missing or malformed records.
- Duplicate detection.
- Verification of time-window boundaries.
- Validation of Bronze-to-Silver transformations.
- Reconciliation of Gold aggregates.
- Testing reruns to confirm idempotency.

## 🔐 Security and Repository Hygiene

The repository should not contain:
- API keys or passwords.
- Local `.env` files.
- Python virtual environments.
- Generated Python cache files.
- Large or confidential raw data files.
- Local database credentials.

Generated data can be recreated by running the pipeline. Consider including a small, anonymized sample dataset when it helps users understand the expected output.

## 🚀 Future Improvements

- Add automated unit and integration tests.
- Implement structured logging and alerting.
- Add data freshness and quality monitoring.
- Improve API rate-limit and error handling.
- Introduce CI checks using GitHub Actions.
- Add documented Power BI dashboards.
- Improve incremental loading and historical backfills.

## 👤 Author

**Your Name**

GitHub: https://github.com/YOUR_USERNAME

## 📄 License

Add an appropriate license if you intend to distribute or reuse the project publicly.
