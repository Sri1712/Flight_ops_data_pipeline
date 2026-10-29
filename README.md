# ✈️ End-to-End Flight Operations Data Engineering Pipeline

An end-to-end data engineering project that ingests live flight data from an external REST API, processes it using Apache Airflow, and organizes it into Bronze, Silver, and Gold layers for analytics and business intelligence.

## 📌 Project Overview

This project demonstrates a production-style batch data pipeline for monitoring global flight activity patterns. It focuses on automated ingestion, data cleaning, incremental processing, orchestration, and analytical data modeling.

The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** to separate raw ingestion, data cleansing, and analytical transformations.

Apache Airflow manages scheduling, task dependencies, retries, and workflow execution. Snowflake serves as the final data warehouse for querying historical flight activity and supporting downstream business intelligence.

## 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| Python | API ingestion and data processing |
| Apache Airflow | Workflow orchestration and scheduling |
| REST API | External flight data source |
| Snowflake | Final cloud data warehouse |
| SQL | Data transformations and analytical modeling |
| Docker | Containerized environment |
| Docker Compose | Local service orchestration |
| Git and GitHub | Version control and source code management |

## 🏗️ Architecture

```text
        External Flight Data REST API
                     |
                     v
             Apache Airflow
           Scheduling and DAGs
                     |
                     v
              Python Ingestion
                     |
                     v
              BRONZE LAYER
               Raw Flight Data
                     |
                     v
              SILVER LAYER
         Cleaned and Normalized Data
                     |
                     v
               GOLD LAYER
          Country-level flight KPIs
                     |
                     v
             SNOWFLAKE WAREHOUSE
           Analytics-Ready Tables
```

## 🥉 Bronze Layer — Raw Data

Airflow task: bronze_ingest
Function: run_bronze_ingest()

The Bronze layer retrieves the current flight state dataset from the OpenSky Network API.

API endpoint: https://opensky-network.org/api/states/all

Responsibilities:
- Preserve raw source records for traceability.
- Organize ingestion by execution time or processing window.
- Support reprocessing when transformations change.

## 🥈 Silver Layer — Data Transformation

Airflow task: silver_transform
Function: run_silver_transform()

The Silver layer reads the Bronze JSON file, converts the flight state vectors into a Pandas DataFrame, selects relevant fields, and writes the result to CSV.

## 🥇 Gold Layer — Analytics

Airflow task: gold_aggregate
Function: run_gold_aggregate()

The Gold layer aggregates the Silver dataset by origin_country to produce country-level flight activity KPIs.

The Gold layer provides a compact dataset suitable for loading into Snowflake and querying for flight activity trends.

## ❄️ Snowflake Data Warehouse

Airflow task: gold_to_snowflake
Function: gold_to_snowflake()

Snowflake is the final destination for the processed and analytics-ready datasets.

Target table: FLIGHTS.KPI.FLIGHT_KPIS
The loader connects to Snowflake using the Airflow connection ID: flight_snowflake

Connection settings are retrieved from the Airflow connection's login, password, and extra configuration fields.

## 🔄 Workflow Orchestration

Apache Airflow coordinates the pipeline through Directed Acyclic Graphs (DAGs).

The workflow is designed to support:
- Scheduled data ingestion.
- Task dependencies.
- Retry policies and failure handling.
- Incremental, time-windowed processing.
- Idempotent task execution.
- Monitoring through Airflow task logs.

## 🔐 Configuration and Security

Configure the required API credentials and Snowflake connection settings through environment variables or Airflow connections, according to the project implementation.

Do not commit:
- API keys or passwords.
- Snowflake credentials or private keys.
- Local `.env` files.
- Virtual environments or Python cache files.
- Large or confidential raw datasets.