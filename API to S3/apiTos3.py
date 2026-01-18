import requests
import boto3
import json
from datetime import datetime

# --- CONFIGURATION ---
API_KEY = ''
BUCKET_NAME = 's3-weather-api'
CITY = 'Manila'

# AWS Credentials (If not configured in ~/.aws/credentials)
# It is better to use environment variables or IAM roles
s3_client = boto3.client(
    's3',
    aws_access_key_id='',
    aws_secret_access_key='',
    region_name='ap-northeast-1' # Change to your bucket region
)

def fetch_and_upload():
    # 1. Fetch data from WeatherAPI
    url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        weather_data = response.json()
        
        # 2. Create a unique filename using a timestamp
        # Format: weather_data/2023-10-27_14-30-Manila.json
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        file_name = f"weather_ultimate/{timestamp}_{CITY}.json"
        
        # 3. Upload to S3
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=json.dumps(weather_data),
                ContentType='application/json'
            )
            print(f"✅ Successfully uploaded {file_name} to {BUCKET_NAME}")
        except Exception as e:
            print(f"❌ AWS Upload Failed: {e}")
    else:
        print(f"❌ WeatherAPI Failed: {response.status_code}")

if __name__ == "__main__":
    fetch_and_upload()