import os
import json
import requests
import boto3
from datetime import datetime


def _require_env(name: str) -> str:
    """Retrieve a required environment variable, raising clearly if absent."""
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(f"Required environment variable '{name}' is not set.")
    return value


def fetch_and_upload():
    # Validate all config at the top so the function fails immediately if anything is missing
    api_key = _require_env("WEATHER_API_KEY")
    bucket_name = _require_env("S3_BUCKET_NAME")
    city = os.environ.get("WEATHER_CITY", "Manila")

    s3_client = boto3.client(
        's3',
        aws_access_key_id=_require_env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_require_env("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION", "ap-northeast-1")
    )

    # 1. Fetch from WeatherAPI with an explicit timeout
    url = f"http://api.weatherapi.com/v1/current.json?key={api_key}&q={city}"
    try:
        response = requests.get(url, timeout=10)
    except requests.exceptions.Timeout:
        raise RuntimeError(f"WeatherAPI request timed out after 10 s for city '{city}'.")
    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(f"Failed to connect to WeatherAPI: {e}") from e

    response.raise_for_status()

    weather_data = response.json()

    # 2. Validate the response has the expected structure before accessing it
    if 'current' not in weather_data:
        raise ValueError(
            f"Unexpected WeatherAPI response — 'current' key missing. "
            f"Keys received: {list(weather_data.keys())}"
        )

    # 3. Upload to S3
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    file_name = f"weather_ultimate/{timestamp}_{city}.json"

    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(weather_data),
        ContentType='application/json'
    )
    print(f"✅ Successfully uploaded {file_name} to {bucket_name}")


if __name__ == "__main__":
    fetch_and_upload()
