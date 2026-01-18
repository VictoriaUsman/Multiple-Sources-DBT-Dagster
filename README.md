<img width="1638" height="1158" alt="7A59B820-46F4-46F2-8465-9F0EFDF2CB09" src="https://github.com/user-attachments/assets/0f1c5aad-c597-46f3-adde-e7083d94a094" />

<img width="2842" height="1564" alt="6764FBD1-9F96-4284-94DC-A30022C27ABC" src="https://github.com/user-attachments/assets/ab011a95-5d2e-4aa2-97f8-3c016377a275" />




🚀 Multi-Source Data Platform with Dagster, dbt & Medallion Architecture

Author: Ian Tristan Cultura
Architecture: End-to-end modern data platform
Orchestration: Dagster
Transformation: dbt
Warehouse: Snowflake
Streaming: Kafka
Deployment: Docker + EC2
CI/CD: GitHub
Monitoring: Flask
Architecture Pattern: Medallion (Bronze / Silver / Gold)

📌 Project Overview

This project implements a production-style data engineering platform that ingests data from multiple sources (CSV, JSON, APIs, forms, streaming), processes them using a Medallion Architecture, and serves clean analytics-ready data to downstream systems.

It is fully containerized using Docker, orchestrated by Dagster, transformed with dbt, and deployed on AWS EC2 with CI/CD automation.

🏗️ High-Level Architecture

Data Sources

📄 CSV uploads

📦 JSON files

🌐 External APIs

📝 Web forms

🔥 Kafka streaming events

Storage & Ingestion

Amazon S3 – raw file landing zone

Google Cloud Storage – external storage integration

Kafka – real-time ingestion

Dagster assets – orchestration of all ingestion steps

🧱 Medallion Architecture (Core Design)
🥉 Bronze Layer

Raw ingested data

No transformations

Schema-on-read

Stored in Snowflake (STAGING)

🥈 Silver Layer

Cleaned & standardized data

Deduplication & type casting

Business logic applied

Managed by dbt models

🥇 Gold Layer

Analytics-ready datasets

Aggregations & metrics

Used for dashboards & downstream apps

Exposed to Redshift / BI / APIs

🔄 Orchestration with Dagster

All pipelines are built as Dagster assets

Dependencies are explicitly defined

Schedules run automatically

Can be triggered manually from Dagster UI

Runs inside Docker containers

Example:

startup_cities_to_snowflake → bronze → silver → gold

🔁 Data Transformation with dbt

dbt models live inside the dbt_project

Snowflake is the main warehouse

dbt handles:

Schema management

Transformations

Testing

Documentation

dbt runs are triggered by Dagster

📦 Technology Stack
Layer	Tools
Orchestration	Dagster
Transformation	dbt
Storage	S3, GCS
Warehouse	Snowflake
Streaming	Kafka
Serving	Redshift, Flask
Containerization	Docker
Deployment	EC2
CI/CD	GitHub Actions
Monitoring	Flask Dashboard
🐳 Dockerized Services

All services run in Docker:

Dagster Webserver

Dagster Daemon

Postgres (Dagster metadata)

Kafka

dbt

Flask monitoring app

Run everything with:

docker compose up --build

🧪 Example Pipeline Flow

User uploads CSV / JSON or submits form

Data lands in S3

Dagster ingests to Snowflake (STAGING)

dbt transforms data (Silver → Gold)

Gold tables are exposed to analytics

Flask monitors pipeline health

CI/CD deploys changes automatically

📊 Monitoring & Observability

Dagster UI for pipeline runs

Flask dashboard for processing status

Logs stored per asset

Retry & failure handling built-in

🚀 Deployment

Fully deployed on AWS EC2

Docker handles environment consistency

Secrets managed with environment variables

GitHub push triggers CI/CD pipeline

🎯 Why This Project Matters

This project demonstrates:

Real-world data engineering architecture

Production-grade orchestration

Multi-source ingestion

Batch + streaming processing

Modern best practices (Medallion, dbt, Dagster)

Cloud-native deployment

CI/CD automation

It is designed as a portfolio-grade project that mirrors what data engineers build in real companies.

📁 Repository Structure
.
├── dagster/
│   ├── assets.py
│   ├── definitions.py
│   └── resources.py
├── dbt_project/
│   ├── models/
│   ├── tests/
│   └── macros/
├── docker-compose.yml
├── flask_app/
├── kafka/
└── README.md

👨‍💻 Author

Ian Tristan Cultura
Data Engineer | Cloud | Analytics Engineering

📌 GitHub: https://github.com/VictoriaUsman

📌 LinkedIn: (add yours here)
