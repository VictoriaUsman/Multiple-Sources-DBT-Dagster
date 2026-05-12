<img width="1638" height="1158" alt="7A59B820-46F4-46F2-8465-9F0EFDF2CB09" src="https://github.com/user-attachments/assets/0f1c5aad-c597-46f3-adde-e7083d94a094" />

<img width="2842" height="1564" alt="6764FBD1-9F96-4284-94DC-A30022C27ABC" src="https://github.com/user-attachments/assets/ab011a95-5d2e-4aa2-97f8-3c016377a275" />

# Multi-Source Data Platform — Dagster · dbt · Snowflake · Kafka

**Author:** Ian Tristan Cultura  
**Architecture:** End-to-end modern data platform  
**Orchestration:** Dagster | **Transformation:** dbt | **Warehouse:** Snowflake  
**Streaming:** Kafka | **Deployment:** Docker + EC2 | **CI/CD:** GitHub Actions  
**Architecture Pattern:** Medallion (Bronze / Silver / Gold)

---

## Project Overview

A production-style data engineering platform that ingests data from multiple sources — CSV files, external APIs, web forms, and Kafka streams — processes them through a Medallion Architecture, and serves clean, analytics-ready data to downstream systems.

Fully containerized with Docker, orchestrated by Dagster, transformed with dbt, and deployed on AWS EC2.

---

## High-Level Architecture

```
Data Sources
├── CSV uploads           → MongoDB → Dagster → Snowflake STAGING
├── WeatherAPI (REST)     → S3 (JSON) → Snowflake STAGING
├── Flask web form        → Kafka topic → Snowflake STAGING
└── Kafka stream events   → Snowflake STAGING

Snowflake Medallion Layers
├── Bronze  — raw source data, no transformations
├── Silver  — cleaned, typed, deduplicated (dbt)
└── Gold    — aggregated, analytics-ready (dbt)
```

---

## Medallion Architecture

### Bronze
- Raw ingested data, schema-on-read
- No transformations; preserves source fidelity
- Loaded into `ULTIMATE.STAGING` via Dagster assets

### Silver
- Standardized column names, trimmed strings, typed fields
- Aggregations scoped to business entities (city-level rollups)
- Managed by dbt models with `CURRENT_TIMESTAMP()` lineage markers

### Gold
- Cross-source join across `silver_startup`, `silver_votertable`, `silver_weather`
- Dynamic column selection via `adapter.get_columns_in_relation`
- Single analytics table (`onebig_table`) consumed by BI / downstream APIs

---

## Error Handling & Validation

This codebase handles failures intentionally — errors surface at the right layer with actionable messages rather than being silently swallowed.

**Environment validation at startup**  
A `_require_env()` helper raises a clear `EnvironmentError` naming the missing variable before any pipeline work begins. No credentials are hardcoded.

**Specific exception types**  
MongoDB failures distinguish `ServerSelectionTimeoutError` (network/host unreachable) from `OperationFailure` (bad credentials). Kafka consumers separate `JSONDecodeError` (malformed message, skip and continue) from `DatabaseError` (Snowflake connection lost, re-raise and stop). Each error type has a different correct response.

**Input & response structure validation**  
- WeatherAPI responses are checked for the `current` key and required nested fields before access — a changed API schema logs which cities were skipped rather than crashing the run.
- CSV files are validated for expected columns after load; missing columns produce a clear diff of what was found vs. what was required.
- Kafka messages are checked for all required fields before any INSERT is attempted.

**Intentional failures over silent bad state**  
- `weather_snapshots` raises `ValueError` if every city fetch fails — an empty pipeline run is treated as a failure, not a success.
- `startup_cities_to_snowflake` raises `ValueError` on an empty MongoDB result rather than loading zero rows silently.
- `s3_to_snowflake_weather` validates AWS credentials exist before building the COPY SQL.

**Resource cleanup**  
MongoDB clients are closed in `finally` blocks regardless of whether extraction succeeds.

**dbt source tests**  
`sources.yml` enforces `not_null` on every key column across all three source tables and `unique` on `STARTUP_CITIES.CITY` — data quality violations fail the dbt run before bad data reaches Silver.

---

## Technology Stack

| Layer | Tools |
|---|---|
| Orchestration | Dagster |
| Transformation | dbt (Snowflake dialect) |
| Warehouse | Snowflake |
| Object Storage | Amazon S3 |
| Streaming | Kafka (Confluent) |
| Web / Form Ingestion | Flask + Flask-WTF |
| Source Database | MongoDB |
| Containerization | Docker Compose |
| Deployment | AWS EC2 |
| CI/CD | GitHub Actions |

---

## Dockerized Services

```
docker compose up --build
```

Starts:
- Dagster Webserver + Daemon
- PostgreSQL (Dagster run storage)
- Kafka broker
- dbt (triggered by Dagster)
- Flask form app

---

## Pipeline Flows

**Startup Cities (Batch)**
```
MongoDB.startups → startup_cities_to_snowflake → bronze_startup → silver_startup → onebig_table
```

**Weather (API → Cloud)**
```
unique_cities → WeatherAPI → S3 (JSON) → s3_to_snowflake_weather → bronze_weather → silver_weather → onebig_table
```

**Voter Submissions (Streaming)**
```
Flask form → Kafka (host_info_topic) → KafkaToSnowflake consumer → VOTERTABLE → bronze_votertable → silver_votertable → onebig_table
```

All three flows converge in the Gold layer `onebig_table`, joined on city.

---

## Repository Structure

```
.
├── dagster/
│   ├── assets.py              # Dagster asset definitions (ETL pipelines)
│   ├── definitions.py         # Resources, schedules, asset loading
│   ├── resources.py           # WeatherAPIResource config class
│   ├── Dockerfile
│   └── multisource/           # dbt project
│       ├── models/
│       │   ├── bronze/        # Raw source views + sources.yml tests
│       │   ├── silver/        # Cleaned & aggregated models
│       │   └── gold/          # onebig_table cross-source join
│       └── macros/            # trim_all_columns, generate_schema_name
├── form/
│   ├── app.py                 # Flask + Kafka producer
│   └── templates/index.html
├── KafkaConsumer/
│   └── KafkaToSnowflake.py    # Kafka consumer → Snowflake loader
├── CSV/
│   └── csv_to_mongo.py        # CSV → MongoDB loader
├── API to S3/
│   └── apiTos3.py             # WeatherAPI → S3 uploader
├── docker-compose.yml
└── pyproject.toml
```

---

## Environment Variables

All secrets are read from environment variables — nothing is hardcoded.

| Variable | Used By |
|---|---|
| `MONGODB_URI` | `assets.py`, `csv_to_mongo.py` |
| `MONGODB_DB` | `assets.py`, `csv_to_mongo.py` |
| `SNOWFLAKE_USER` | `assets.py`, `KafkaToSnowflake.py` |
| `SNOWFLAKE_PASSWORD` | `assets.py`, `KafkaToSnowflake.py` |
| `SNOWFLAKE_ACCOUNT` | `assets.py`, `KafkaToSnowflake.py` |
| `SNOWFLAKE_DATABASE` | `assets.py`, `KafkaToSnowflake.py` |
| `SNOWFLAKE_WAREHOUSE` | `assets.py`, `KafkaToSnowflake.py` |
| `AWS_ACCESS_KEY_ID` | `assets.py`, `apiTos3.py` |
| `AWS_SECRET_ACCESS_KEY` | `assets.py`, `apiTos3.py` |
| `WEATHER_API_KEY` | `apiTos3.py` |
| `S3_BUCKET_NAME` | `apiTos3.py` |
| `SECRET_KEY` | `form/app.py` |

---

## Author

**Ian Tristan Cultura** — Data Engineer · Cloud · Analytics Engineering

GitHub: https://github.com/VictoriaUsman
