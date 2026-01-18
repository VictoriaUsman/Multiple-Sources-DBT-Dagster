from kafka import KafkaConsumer
import json
import snowflake.connector
from datetime import datetime, timezone
import time

# --- 1. CONFIG ---
TOPIC = "host_info_topic"
BROKER = "localhost:9094"

print(f"Connecting to Kafka at {BROKER}...")

consumer = KafkaConsumer(
    "host_info_topic",
    bootstrap_servers="127.0.0.1:9094", # Using IP is more reliable in Python 3.12
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="snowflake-loader-v101",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    # THIS LINE IS THE KEY:
    api_version=(3, 5, 0) 
)

# --- 2. CONNECTIVITY CHECK ---
if consumer.bootstrap_connected():
    print("✅ Successfully connected to Kafka Broker!")
else:
    print("❌ Failed to connect to Broker. Check if Docker is running.")
    exit()

print(f"Subscribed to: {consumer.subscription()}")
print("Waiting for messages... (Press Ctrl+C to stop)")

# --- 3. LOOP ---
try:
    while True:
        # We use poll instead of a for-loop to see the "Heartbeat"
        records = consumer.poll(timeout_ms=500)
        
        if not records:
            # This prints every 0.5s so we know the script is alive
            # print(".", end="", flush=True) 
            continue

        print(f"\n📢 Found {len(records)} messages!")
        for tp, messages in records.items():
            for msg in messages:
                data = msg.value
                print(f"📥 Received: {data}")
                
                # Snowflake logic here...
                # cursor.execute(...)
                # conn.commit()

except KeyboardInterrupt:
    print("\nStopping...")