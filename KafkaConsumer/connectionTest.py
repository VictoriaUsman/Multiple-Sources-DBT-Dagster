import json
from kafka import KafkaConsumer
import sys

# Since 9092 succeeded in your nc test, let's use that.
BROKER = "127.0.0.1:9094" 

print(f"Connecting to Kafka at {BROKER}...")

try:
    consumer = KafkaConsumer(
        "host_info_topic",
        bootstrap_servers=[BROKER],
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        api_version=(3, 5, 0),
        # EXTENDED TIMEOUTS FOR MAC/DOCKER
        request_timeout_ms=60000,      # 60 seconds
        connections_max_idle_ms=60000,
        metadata_max_age_ms=60000,
        # Default behavior
        auto_offset_reset="earliest",
        group_id="snowflake-loader-v101"
    )
    
    # Force a metadata refresh to check connection
    if consumer.bootstrap_connected():
        print("✅ Successfully connected to Kafka Broker!")
        print(f"Subscribed to: {consumer.subscription()}")
    else:
        print("❌ Connection object created, but bootstrap failed.")

except Exception as e:
    print(f"❌ Python Error: {e}")
    sys.exit(1)

# --- 3. LOOP ---
try:
    print("Waiting for messages... (Press Ctrl+C to stop)")
    for msg in consumer:
        print(f"📥 Received: {msg.value}")
except KeyboardInterrupt:
    print("\nStopping...")