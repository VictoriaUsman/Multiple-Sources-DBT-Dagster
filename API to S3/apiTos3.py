import os
import json
import requests
import boto3
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from botocore.config import Config


def _require_env(name: str) -> str:
    """Retrieve a required environment variable, raising clearly if absent."""
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(f"Required environment variable '{name}' is not set.")
    return value


def _build_http_session(retries: int = 3, backoff_factor: float = 1.0) -> requests.Session:
    """Return a Session that automatically retries on transient HTTP errors.

    backoff_factor controls the sleep between retries: {backoff_factor} * (2 ^ (attempt - 1)).
    status_forcelist covers rate-limiting (429) and all common server errors (5xx).
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch_and_upload():
    api_key = _require_env("WEATHER_API_KEY")
    bucket_name = _require_env("S3_BUCKET_NAME")
    city = os.environ.get("WEATHER_CITY", "Manila")

    # boto3 adaptive retry mode backs off automatically on throttling and transient errors.
    s3_client = boto3.client(
        's3',
        aws_access_key_id=_require_env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_require_env("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION", "ap-northeast-1"),
        config=Config(retries={'max_attempts': 3, 'mode': 'adaptive'}),
    )

    # 1. Fetch from WeatherAPI — session handles retries and backoff transparently
    session = _build_http_session(retries=3, backoff_factor=1.0)
    url = f"http://api.weatherapi.com/v1/current.json?key={api_key}&q={city}"

    try:
        response = session.get(url, timeout=10)
    except requests.exceptions.Timeout:
        raise RuntimeError(f"WeatherAPI request timed out after 10s for city '{city}'.")
    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(f"Failed to connect to WeatherAPI: {e}") from e

    response.raise_for_status()

    weather_data = response.json()

    # 2. Validate response structure before accessing nested keys
    if 'current' not in weather_data:
        raise ValueError(
            f"Unexpected WeatherAPI response — 'current' key missing. "
            f"Keys received: {list(weather_data.keys())}"
        )

    # 3. Upload to S3 — boto3 adaptive retry handles transient S3 errors
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
