import json
import os
import time
import snowflake.connector
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
from datetime import datetime, timezone


def _require_env(name: str) -> str:
    """Retrieve a required environment variable, raising clearly if absent."""
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(f"Required environment variable '{name}' is not set.")
    return value


# --- 1. KAFKA CONFIG ---
conf = {
    'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9094'),
    'group.id': 'manual_fix_group_99',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'broker.address.family': 'v4'
}

consumer = Consumer(conf)

# DIRECT ASSIGNMENT: Skip the "Waiting" phase
tp = TopicPartition('host_info_topic', 0, OFFSET_BEGINNING)
consumer.assign([tp])

# --- 2. SNOWFLAKE CONFIG ---
conn = snowflake.connector.connect(
    user=_require_env("SNOWFLAKE_USER"),
    password=_require_env("SNOWFLAKE_PASSWORD"),
    account=_require_env("SNOWFLAKE_ACCOUNT"),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ultimate"),
    database=os.environ.get("SNOWFLAKE_DATABASE", "ultimate"),
    schema=os.environ.get("SNOWFLAKE_SCHEMA", "staging"),
)
cursor = conn.cursor()

print("✅ Manual Assignment Successful.")
print("🚀 Reading directly from Partition 0... please wait 5 seconds...")

REQUIRED_FIELDS = ('hostname', 'country', 'city', 'contact')

# --- 3. MAIN LOOP ---
try:
    consumer.poll(1.0)

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"\n❌ Kafka Error: {msg.error()}")
            continue

        # Isolate JSON decode errors from downstream processing
        try:
            data = json.loads(msg.value().decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"❌ Malformed message at offset {msg.offset()}: {e} — skipping.")
            continue

        # Validate all required fields are present and non-empty before touching Snowflake
        missing = [f for f in REQUIRED_FIELDS if not data.get(f)]
        if missing:
            print(f"⚠️  Message at offset {msg.offset()} missing required fields {missing} — skipping.")
            continue

        print(f"\n📥 RECEIVED: {data['hostname']}")

        # Retry the INSERT on transient connection errors with exponential backoff.
        # ProgrammingError (bad schema, type mismatch) is not retried — it won't fix itself.
        # DatabaseError (connection dropped) is retried up to 3 times before re-raising.
        for attempt in range(1, 4):
            try:
                cursor.execute(
                    "INSERT INTO votertable (voter, country, city, contact_number, created_at) VALUES (%s, %s, %s, %s, %s)",
                    (data['hostname'], data['country'], data['city'], data['contact'], datetime.now(timezone.utc))
                )
                conn.commit()
                print(f"❄️ PUSHED TO SNOWFLAKE: {data['hostname']}")
                break
            except snowflake.connector.errors.ProgrammingError as e:
                print(f"❌ Snowflake query error at offset {msg.offset()}: {e}")
                break  # Schema/syntax errors won't resolve on retry
            except snowflake.connector.errors.DatabaseError as e:
                if attempt == 3:
                    print(f"❌ Snowflake connection failed after 3 attempts: {e}")
                    raise
                delay = 2 ** (attempt - 1)
                print(f"⚠️  Snowflake error (attempt {attempt}/3): {e}. Retrying in {delay}s...")
                time.sleep(delay)

except KeyboardInterrupt:
    print("\n🛑 Stopping...")
finally:
    consumer.close()
    cursor.close()
    conn.close()
