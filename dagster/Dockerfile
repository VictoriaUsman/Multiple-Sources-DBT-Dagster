FROM python:3.12-slim

# 1. Install system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/dagster_home
# 2. Install all Python libraries (Added dbt-related packages)
RUN pip install --no-cache-dir \
    dagster \
    dagster-webserver \
    dagster-postgres \
    dagster-aws \
    dagster-snowflake \
    dagster-dbt \
    dbt-core \
    dbt-snowflake \
    snowflake-connector-python \
    requests \
    pymongo \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    certifi \
    snowflake-sqlalchemy 

# 3. Copy your local code
COPY . /opt/dagster/dagster_home

ENV DBT_PROFILES_DIR=/opt/dagster/dagster_home/multisource
RUN dbt parse --project-dir /opt/dagster/dagster_home/multisource --profiles-dir /opt/dagster/dagster_home/multisource