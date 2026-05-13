from dagster import asset, Output, AssetExecutionContext
import json
import os
import random
import subprocess
import time
from datetime import datetime
from dagster_aws.s3 import S3Resource
from dagster_snowflake import SnowflakeResource
from resources import WeatherAPIResource
from pymongo import MongoClient, errors as pymongo_errors
from botocore.exceptions import ClientError as BotoCoreClientError
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Any
from dagster_dbt import dbt_assets, DbtCliResource
from pathlib import Path

def _require_env(name: str) -> str:
    """Retrieve a required environment variable, raising clearly if absent."""
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(f"Required environment variable '{name}' is not set.")
    return value


class _RetryableError(Exception):
    """Signals a transient failure that the caller should retry."""


def _retry_with_backoff(func, *, retries=3, base_delay=1.0, retryable=(_RetryableError,), log=None):
    """Retry func up to `retries` times with exponential backoff and jitter.

    base_delay doubles on each attempt (1s → 2s → 4s by default).
    A small random jitter is added to prevent thundering-herd when many
    cities fail simultaneously and all retry at the same interval.
    """
    for attempt in range(1, retries + 1):
        try:
            return func()
        except retryable as e:
            if attempt == retries:
                raise
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.3)
            if log:
                log.warning(f"Attempt {attempt}/{retries} failed: {e}. Retrying in {delay:.1f}s...")
            time.sleep(delay)


# 1. Setup paths
DBT_PROJECT_DIR = Path("/opt/dagster/dagster_home/multisource")
DBT_MANIFEST_PATH = DBT_PROJECT_DIR.joinpath("target", "manifest.json")

# 2. FORCE VALID MANIFEST GENERATION AT STARTUP
# This ensures that even if the volume hasn't synced, the container 
# builds its own valid manifest.json so the 'metrics' KeyError disappears.
if not DBT_MANIFEST_PATH.exists():
    os.makedirs(DBT_MANIFEST_PATH.parent, exist_ok=True)
    # Use subprocess to run dbt parse inside the container
    subprocess.run(
        ["dbt", "parse", "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
        check=True
    )

@dbt_assets(manifest=DBT_MANIFEST_PATH)
def multisource_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(compute_kind="snowflake", group_name="weather_etl")
def unique_cities(snowflake: SnowflakeResource, startup_cities_to_snowflake):
    """Query Snowflake for cities present in host_info."""
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT CITY FROM ULTIMATE.STAGING.STARTUP_CITIES WHERE CITY IS NOT NULL;")
        return [row[0] for row in cursor.fetchall()]

@asset(compute_kind="python", group_name="weather_etl")
def weather_snapshots(
    context: AssetExecutionContext,
    unique_cities: list, 
    weather_api: WeatherAPIResource, 
    s3: S3Resource
):
    """Fetch weather and upload batch to S3."""
    if not unique_cities:
        raise ValueError("No cities to process — upstream asset returned an empty list.")

    # Configurable delay between city requests to stay within WeatherAPI rate limits.
    # Default 0.5s gives ~2 req/s, well within the free-tier limit of 1M calls/month.
    rate_limit_delay = float(os.environ.get("WEATHER_API_RATE_LIMIT_DELAY", "0.5"))

    weather_reports = []
    failed_cities = []

    for city in unique_cities:
        # Wrap the fetch in a helper so _retry_with_backoff can call it repeatedly.
        # 429 (rate-limited) and 5xx (server errors) are transient — worth retrying.
        # 4xx client errors are not retried; they fall through to the status check below.
        def _fetch(city=city):
            res = weather_api.fetch(city)
            if res.status_code in (429, 500, 502, 503, 504):
                raise _RetryableError(f"HTTP {res.status_code} for '{city}'")
            return res

        try:
            res = _retry_with_backoff(_fetch, retries=3, base_delay=2.0, log=context.log)
        except _RetryableError as e:
            context.log.warning(f"Weather API transient failure for '{city}' after 3 attempts: {e} — skipping.")
            failed_cities.append(city)
            continue
        except Exception as e:
            context.log.warning(f"Unexpected error fetching weather for '{city}': {e} — skipping.")
            failed_cities.append(city)
            continue

        if res.status_code != 200:
            context.log.warning(f"Weather API returned {res.status_code} for '{city}' — skipping.")
            failed_cities.append(city)
            continue

        data = res.json()
        current = data.get('current')
        if not current or 'temp_c' not in current or 'condition' not in current:
            context.log.warning(f"Unexpected API response structure for '{city}' — skipping.")
            failed_cities.append(city)
            continue

        weather_reports.append({
            "city": city,
            "temp_c": current['temp_c'],
            "condition": current['condition']['text'],
            "extracted_at": datetime.now().isoformat()
        })

        time.sleep(rate_limit_delay)

    if failed_cities:
        context.log.warning(f"Skipped {len(failed_cities)} cities due to API errors: {failed_cities}")

    if not weather_reports:
        raise ValueError(f"No weather data collected — all {len(unique_cities)} cities failed.")

    s3_key = f"weather_snapshots/batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        _retry_with_backoff(
            lambda: s3.get_client().put_object(
                Bucket='s3-weather-api',
                Key=s3_key,
                Body=json.dumps(weather_reports, indent=4),
                ContentType='application/json'
            ),
            retries=3,
            base_delay=1.0,
            retryable=(BotoCoreClientError,),
            log=context.log
        )
    except BotoCoreClientError as e:
        raise RuntimeError(f"S3 upload failed after 3 retries: {e}") from e

    context.add_output_metadata({
        "cities_processed": len(weather_reports),
        "s3_key": s3_key
    })

    return weather_reports

@asset(
    compute_kind="snowflake",
    group_name="weather_etl"
)
def s3_to_snowflake_weather(
    context: AssetExecutionContext, 
    snowflake_staging: SnowflakeResource, # Change this from 'snowflake' to 'snowflake_staging'
    weather_snapshots: list 
):
    """Tells Snowflake to copy the JSON data directly from S3 into the STAGING schema."""
    
    s3_bucket = "s3-weather-api"
    s3_path = f"s3://{s3_bucket}/weather_snapshots/"
    
    if not weather_snapshots:
        raise ValueError("No weather rows to load — upstream asset produced no data.")

    aws_id = _require_env("AWS_ACCESS_KEY_ID")
    aws_key = _require_env("AWS_SECRET_ACCESS_KEY")

    copy_sql = f"""
        COPY INTO ULTIMATE.STAGING.WEATHER_SNAPSHOTS
        FROM '{s3_path}'
        CREDENTIALS = (
            AWS_KEY_ID = '{aws_id}'
            AWS_SECRET_KEY = '{aws_key}'
        )
        FILE_FORMAT = (
            TYPE = 'JSON' 
            STRIP_OUTER_ARRAY = TRUE
        )
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    """

    # Use the 'snowflake_staging' resource here
    with snowflake_staging.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(copy_sql)
        
        results = cursor.fetchall()
        failed = [row for row in results if row[1] != "LOADED"]
        if failed:
            raise RuntimeError(f"Snowflake COPY INTO had {len(failed)} failed file(s): {failed}")
        for row in results:
            context.log.info(f"File: {row[0]} | Status: {row[1]} | Rows Loaded: {row[3]}")
            
    context.add_output_metadata({"rows_synced": len(weather_snapshots)})





@asset(group_name="startup_etl")
def startup_cities_to_snowflake(context: AssetExecutionContext):
    """
    Extracts data from MongoDB and loads it into Snowflake with strictly cleaned identifiers.
    """
    # --- 1. MONGODB EXTRACTION ---
    uri = _require_env("MONGODB_URI")
    db_name = os.environ.get("MONGODB_DB", "StartUpCities_Postgres_bellmudbit")

    context.log.info("Connecting to MongoDB...")
    try:
        client = MongoClient(uri, authSource=db_name, tls=False, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
    except pymongo_errors.ServerSelectionTimeoutError as e:
        raise RuntimeError(f"Could not reach MongoDB within timeout: {e}") from e
    except pymongo_errors.OperationFailure as e:
        raise RuntimeError(f"MongoDB authentication failed: {e}") from e

    try:
        db = client[db_name]
        df = pd.DataFrame(list(db['startups'].find()))
    finally:
        client.close()

    if df.empty:
        raise ValueError(f"No documents found in MongoDB collection '{db_name}.startups'.")

    # --- 2. DATA CLEANING (STRICT SNOWFLAKE NAMING) ---
    if '_id' in df.columns:
        df = df.drop(columns=['_id'])
    
    # 1. Convert to Uppercase
    # 2. Replace all non-alphanumeric chars (spaces, dots, hyphens) with underscores
    # 3. Ensure name doesn't start with a number (Snowflake requirement)
    clean_cols = []
    for i, col in enumerate(df.columns):
        c = str(col).strip().upper()
        c = c.replace(' ', '_').replace('.', '_').replace('-', '_')
        if not c:
            c = f"COL_{i}"
        elif c[0].isdigit():
            c = f"YR_{c}"
        clean_cols.append(c)
    
    df.columns = clean_cols
    
    # Map the specific column causing issues to a shorter, safe name
    # We rename 'CHANGE_IN_POSITION_FROM_2020' to 'CHG_POS_2020' for reliability
    df = df.rename(columns={
        'CHANGE_IN_POSITION_FROM_2020': 'CHG_POS_2020',
        'QUATITY_SCORE': 'QUANTITY_SCORE'
    })

    # --- 3. SNOWFLAKE LOAD ---
    sf_user = _require_env("SNOWFLAKE_USER")
    sf_password = _require_env("SNOWFLAKE_PASSWORD")
    sf_account = _require_env("SNOWFLAKE_ACCOUNT")
    sf_db = os.environ.get("SNOWFLAKE_DATABASE", "ULTIMATE")
    sf_schema = os.environ.get("SNOWFLAKE_SCHEMA", "STAGING")
    sf_wh = os.environ.get("SNOWFLAKE_WAREHOUSE", "ULTIMATE")

    connection_string = (
        f"snowflake://{sf_user}:{sf_password}@{sf_account}/"
        f"{sf_db}/{sf_schema}?warehouse={sf_wh}&role=ACCOUNTADMIN"
    )
    
    engine = create_engine(connection_string)

    with engine.connect() as conn:
        context.log.info("Creating Table structure in Snowflake...")
        # Note: We use the exact cleaned names here
        conn.execute(text(f"""
            CREATE OR REPLACE TABLE {sf_db}.{sf_schema}.STARTUP_CITIES (
                POSITION NUMBER,
                CHANGE_IN_POSITION NUMBER,
                CITY STRING,
                COUNTRY STRING,
                TOTAL_SCORE FLOAT,
                QUANTITY_SCORE FLOAT,
                QUALITY_SCORE FLOAT,
                BUSINESS_SCORE FLOAT,
                SIGN_OF_CHANGE_IN_POSITION STRING,
                CHG_POS_2020 STRING
            )
        """))
        conn.commit()

        context.log.info("Pushing data to Snowflake...")
        df.to_sql(
            name="STARTUP_CITIES",
            con=conn,
            schema=sf_schema,
            if_exists="append", # Table is already created above
            index=False,
            method="multi"
        )
        conn.commit()

    context.log.info("✅ Pipeline Complete!")
    return Output(value=df.head(), metadata={"rows": len(df)})