import os
import pandas as pd
from pathlib import Path
from pymongo import MongoClient, errors as pymongo_errors


EXPECTED_COLUMNS = {'Position', 'City', 'Country', 'Total Score'}


def push_csv_to_mongodb(file_path: str = None):
    uri = os.environ.get("MONGODB_URI")
    if not uri:
        raise EnvironmentError("Required environment variable 'MONGODB_URI' is not set.")

    db_name = os.environ.get("MONGODB_DB", "StartUpCities_Postgres_bellmudbit")

    csv_path = Path(file_path or os.environ.get("CSV_FILE_PATH", ""))
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: '{csv_path}'.")

    # 1. Read and validate CSV structure before touching MongoDB
    try:
        df = pd.read_csv(csv_path)
    except pd.errors.ParserError as e:
        raise ValueError(f"Could not parse CSV file '{csv_path}': {e}") from e

    missing_cols = EXPECTED_COLUMNS - set(df.columns)
    if missing_cols:
        raise ValueError(
            f"CSV is missing expected columns: {missing_cols}. "
            f"Columns found: {list(df.columns)}"
        )

    df.columns = [c.replace(' ', '_').replace('.', '') for c in df.columns]
    data_dict = df.to_dict('records')

    print(f"Read {len(data_dict)} rows from '{csv_path}'.")

    # 2. Connect and authenticate before attempting any writes
    try:
        client = MongoClient(uri, authSource=db_name, tls=False, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("✅ Connected to MongoDB.")
    except pymongo_errors.ServerSelectionTimeoutError as e:
        raise RuntimeError(f"Could not reach MongoDB within timeout: {e}") from e
    except pymongo_errors.OperationFailure as e:
        raise RuntimeError(f"MongoDB authentication failed: {e}") from e

    try:
        collection = client[db_name]['startups']
        print(f"Uploading {len(data_dict)} rows...")
        collection.insert_many(data_dict)
        print("✅ Data loaded into MongoDB.")
    except pymongo_errors.BulkWriteError as e:
        raise RuntimeError(f"Bulk write failed — {e.details['nInserted']} rows inserted before error.") from e
    finally:
        client.close()


if __name__ == "__main__":
    push_csv_to_mongodb()
