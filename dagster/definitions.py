import sys
import os
from pathlib import Path
from dagster import Definitions, EnvVar, load_assets_from_modules, AssetSelection, ScheduleDefinition, run_failure_sensor, RunFailureSensorContext
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

# 2. Discord failure sensor
@run_failure_sensor
def discord_on_run_failure(context: RunFailureSensorContext, discord: resources.DiscordResource):
    message = (
        f":red_circle: **Dagster run failed**\n"
        f"**Job:** {context.dagster_run.job_name}\n"
        f"**Run ID:** `{context.dagster_run.run_id}`\n"
        f"**Error:** {context.failure_event.message}"
    )
    discord.notify(message)


# 3. Define the Selection and Schedule
# This selects 'startup_cities_to_snowflake' and everything that depends on it
full_pipeline_selection = AssetSelection.assets("startup_cities_to_snowflake").downstream()

end_to_end_schedule = ScheduleDefinition(
    name="full_medallion_sync",
    target=full_pipeline_selection,
    cron_schedule="0 8 * * *", 
)

# 4. Load ALL assets from the assets module
# This automatically includes both @asset and @dbt_assets
all_assets = load_assets_from_modules([assets])

# 5. Final Definitions object
defs = Definitions(
    assets=all_assets,
    schedules=[end_to_end_schedule],
    sensors=[discord_on_run_failure],
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
        "discord": resources.DiscordResource(
            webhook_url=EnvVar("DISCORD_WEBHOOK_URL")
        ),
    },
)