import sys
import os
from pathlib import Path
from dagster import Definitions, EnvVar, load_assets_from_modules, AssetSelection, ScheduleDefinition
from dagster_aws.s3 import S3Resource
from dagster_snowflake import SnowflakeResource
from dagster_dbt import DbtCliResource

# --- DOCKER FIX: Ensure Python finds your modules ---
# Point to the root of the mounted Dagster home
sys.path.append("/opt/dagster/dagster_home")

import assets
import resources

# 1. Define the dbt Path inside the container
DBT_PROJECT_DIR = Path("/opt/dagster/dagster_home/multisource")

# 2. Define the Selection and Schedule
# This selects 'startup_cities_to_snowflake' and everything that depends on it
full_pipeline_selection = AssetSelection.assets("startup_cities_to_snowflake").downstream()

end_to_end_schedule = ScheduleDefinition(
    name="full_medallion_sync",
    target=full_pipeline_selection,
    cron_schedule="0 8 * * *", 
)

# 3. Load ALL assets from the assets module
# This automatically includes both @asset and @dbt_assets
all_assets = load_assets_from_modules([assets])

# 4. Final Definitions object
defs = Definitions(
    assets=all_assets,  # This list already includes your dbt assets
    schedules=[end_to_end_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR)),
        "s3": S3Resource(   
            aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
            region_name="us-east-1",
        ),
        "snowflake": SnowflakeResource(
            account="PONEVOV-ZF78227",
            user="iantrisdc",
            password=EnvVar("SNOWFLAKE_PASSWORD"),
            database="ULTIMATE",
            warehouse="ULTIMATE",
            schema="PUBLIC",
        ),
        "snowflake_staging": SnowflakeResource(
            account="PONEVOV-ZF78227",
            user="iantrisdc",
            password=EnvVar("SNOWFLAKE_PASSWORD"),
            database="ULTIMATE",
            warehouse="ULTIMATE",
            schema="STAGING",
        ),
        "weather_api": resources.WeatherAPIResource(
            api_key=EnvVar("WEATHER_API_KEY")
        ),
    },
)