import pandas as pd
from pymongo import MongoClient
import os

def push_csv_to_mongodb():
    # 1. Your Exact URI from Filess.io
    # I have cleaned this up to ensure Python parses it correctly
    uri = "mongodb://StartUpCities_Postgres_bellmudbit:5b5ae1d74a806ecd67359e23975a4b5ce07737b4@6jtrct.h.filess.io:27018/StartUpCities_Postgres_bellmudbit"

    try:
        print("Connecting to Filess.io...")
        
        # We specify authSource as the database name itself
        client = MongoClient(
            uri,
            authSource="StartUpCities_Postgres_bellmudbit",
            tls=False,  # Stick with False since port 27018 worked this way
            serverSelectionTimeoutMS=5000
        )

        # Test if the password is accepted
        client.admin.command('ping')
        print("✅ SUCCESS: Authenticated and Connected!")

        # 2. Select the Database and Collection
        db = client['StartUpCities_Postgres_bellmudbit']
        collection = db['startups']

        # 3. Process the CSV
        full_path = "/Users/iantristancultura/Documents/Ultimate Project/CSV/Best Cities for Startups.csv"
        
        print(f"Reading {full_path}...")
        df = pd.read_csv(full_path)
        
        # Clean column names (MongoDB doesn't like dots or spaces in keys)
        df.columns = [c.replace(' ', '_').replace('.', '') for c in df.columns]
        data_dict = df.to_dict('records')

        # 4. Final Push
        print(f"Uploading {len(data_dict)} rows...")
        collection.insert_many(data_dict)
        print("✅ MISSION ACCOMPLISHED: Data is in MongoDB!")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n💡 Check: Did you create the user 'StartUpCities_Postgres_bellmudbit' in the Filess.io dashboard?")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    push_csv_to_mongodb()