import json
from dagster import Definitions, asset, sensor, RunRequest, SensorEvaluationContext
from dagster_kafka import KafkaResource

# 1. Kafka Resource
kafka_resource = KafkaResource(
    bootstrap_servers="kafka:9092"
)

# 2. The Snowflake Asset (Placeholder for now)
@asset
def snowflake_host_info(context):
    context.log.info("Processing data for Snowflake...")
    return True

# 3. The Kafka Sensor
@sensor(target=snowflake_host_info)
def kafka_host_sensor(context: SensorEvaluationContext):
    consumer = kafka_resource.get_consumer(group_id="dagster_consumer_group")
    consumer.subscribe(["host_info_topic"])
    
    # Check for 1 message
    message = consumer.poll(timeout=1.0)
    
    if message is not None and message.error() is None:
        data = json.loads(message.value().decode("utf-8"))
        context.log.info(f"Found new host: {data.get('hostname')}")
        yield RunRequest(run_key=f"msg_{message.offset()}")
    
    consumer.close()

defs = Definitions(
    assets=[snowflake_host_info],
    sensors=[kafka_host_sensor],
    resources={"kafka": kafka_resource}
)