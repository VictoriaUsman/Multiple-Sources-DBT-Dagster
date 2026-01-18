from pymongo import MongoClient

uri = "mongodb://StartUpCities_Postgres_bellmudbit:5b5ae1d74a806ecd67359e23975a4b5ce07737b4@6jtrct.h.filess.io:27018/StartUpCities_Postgres_bellmudbit"
client = MongoClient(uri, authSource="StartUpCities_Postgres_bellmudbit", tls=False)
db = client['StartUpCities_Postgres_bellmudbit']
collection = db['startups']

try:
    # 1. Get the IDs of the first 10 documents
    # We use the default _id which is usually sorted by insertion time
    keep_docs = list(collection.find({}, {"_id": 1}).limit(10))
    keep_ids = [doc["_id"] for doc in keep_docs]

    if not keep_ids:
        print("The collection is already empty!")
    else:
        # 2. Delete all documents whose ID is NOT in the 'keep' list
        result = collection.delete_many({"_id": {"$nin": keep_ids}})
        
        print(f"✅ Successfully kept 10 rows.")
        print(f"🗑️ Deleted {result.deleted_count} rows from position 11 to the end.")

except Exception as e:
    print(f"❌ Error: {e}")
finally:
    client.close()