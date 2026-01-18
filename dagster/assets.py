from dagster import asset, Output, AssetExecutionContext
import json
import os
import subprocess
from datetime import datetime
from dagster_aws.s3 import S3Resource
from dagster_snowflake import SnowflakeResource
from resources import WeatherAPIResource
from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Any
from dagster_dbt import dbt_assets, DbtCliResource
from pathlib import Path

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
    weather_reports = []
    for city in unique_cities:
        res = weather_api.fetch(city)
        if res.status_code == 200:
            data = res.json()
            weather_reports.append({
                "city": city,
                "temp_c": data['current']['temp_c'],
                "condition": data['current']['condition']['text'],
                "extracted_at": datetime.now().isoformat()
            })

    if weather_reports:
        # Generate a timestamped key
        s3_key = f"weather_snapshots/batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        s3.get_client().put_object(
            Bucket='s3-weather-api',
            Key=s3_key,
            Body=json.dumps(weather_reports, indent=4),
            ContentType='application/json'
        )
        context.add_output_metadata({
            "cities_processed": len(weather_reports), 
            "s3_key": s3_key
        })
    
    # Return the data so the downstream asset can see how many rows to expect
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
    
    aws_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    # Fully qualifying the table name with the STAGING schema for clarity
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
        for row in results:
            context.log.info(f"File: {row[0]} | Status: {row[1]} | Rows Loaded: {row[3]}")
            
    context.add_output_metadata({"rows_synced": len(weather_snapshots)})





@asset(group_name="startup_etl")
def startup_cities_to_snowflake(context: AssetExecutionContext):
    """
    Extracts data from MongoDB and loads it into Snowflake with strictly cleaned identifiers.
    """
    # --- 1. MONGODB EXTRACTION ---
    uri = "mongodb://StartUpCities_Postgres_bellmudbit:5b5ae1d74a806ecd67359e23975a4b5ce07737b4@6jtrct.h.filess.io:27018/StartUpCities_Postgres_bellmudbit"
    
    context.log.info("Connecting to MongoDB...")
    client = MongoClient(uri, authSource="StartUpCities_Postgres_bellmudbit", tls=False)
    db = client['StartUpCities_Postgres_bellmudbit']
    df = pd.DataFrame(list(db['startups'].find()))
    client.close()

    if df.empty:
        raise Exception("No data found in MongoDB!")

    # --- 2. DATA CLEANING (STRICT SNOWFLAKE NAMING) ---
    if '_id' in df.columns:
        df = df.drop(columns=['_id'])
    
    # 1. Convert to Uppercase
    # 2. Replace all non-alphanumeric chars (spaces, dots, hyphens) with underscores
    # 3. Ensure name doesn't start with a number (Snowflake requirement)
    clean_cols = []
    for col in df.columns:
        # Remove any leading/trailing whitespace
        c = str(col).strip().upper()
        # Replace problematic characters
        c = c.replace(' ', '_').replace('.', '_').replace('-', '_')
        # Final safety check: if it starts with a digit, prefix it
        if c[0].isdigit():
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
    sf_user = "iantrisdc"
    sf_password = "NL6pGafqzKy6Xrj"
    sf_account = "PONEVOV-ZF78227"
    sf_db = "ULTIMATE"
    sf_schema = "STAGING"
    sf_wh = "ULTIMATE"

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