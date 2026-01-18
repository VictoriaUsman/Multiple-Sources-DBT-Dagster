import json
import snowflake.connector
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
from datetime import datetime, timezone

# --- 1. KAFKA CONFIG ---
conf = {
    'bootstrap.servers': '127.0.0.1:9094',
    'group.id': 'manual_fix_group_99',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'broker.address.family': 'v4'
}

consumer = Consumer(conf)

# DIRECT ASSIGNMENT: Skip the "Waiting" phase
# We tell it: Go to host_info_topic, Partition 0, and start at the beginning
tp = TopicPartition('host_info_topic', 0, OFFSET_BEGINNING)
consumer.assign([tp])

# --- 2. SNOWFLAKE CONFIG ---
conn = snowflake.connector.connect(
    user="iantrisdc",
    password="NL6pGafqzKy6Xrj",
    account="PONEVOV-ZF78227",
    warehouse="ultimate",
    database="ultimate",
    schema="staging",
)
cursor = conn.cursor()

print("✅ Manual Assignment Successful.")
print("🚀 Reading directly from Partition 0... please wait 5 seconds...")

# --- 3. MAIN LOOP ---
try:
    # Give it one initial poll to stabilize the connection
    consumer.poll(1.0)
    
    while True:
        msg = consumer.poll(1.0) 

        if msg is None:
            continue
            
        if msg.error():
            print(f"\n❌ Kafka Error: {msg.error()}")
            continue

        # DATA RETRIEVED!
        data = json.loads(msg.value().decode('utf-8'))
        print(f"\n📥 RECEIVED: {data['hostname']}")

        try:
            cursor.execute(
                "INSERT INTO votertable (voter, country, city, contact_number, created_at) VALUES (%s, %s, %s, %s, %s)",
                (data.get('hostname'), data.get('country'), data.get('city'), data.get('contact'), datetime.now(timezone.utc))
            )
            conn.commit()
            print(f"❄️ PUSHED TO SNOWFLAKE: {data['hostname']}")
        except Exception as e:
            print(f"❌ Snowflake Error: {e}")

except KeyboardInterrupt:
    print("\n🛑 Stopping...")
finally:
    consumer.close()
    cursor.close()
    conn.close()