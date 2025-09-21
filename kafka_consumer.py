import json
import os
from kafka import KafkaConsumer

# Specify Kafka cluster parameters
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TEST = os.environ.get("KAFKA_TOPIC_TEST", "vehicle_positions")
KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "7.3.1")

if __name__ == '__main__':
    consumer = KafkaConsumer(
        KAFKA_TOPIC_TEST,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        api_version=KAFKA_API_VERSION,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("Consumer started, waiting for messages...")
    cnt=0
    for message in consumer:
        print(f"message: {message.value}")
        cnt += 1

    print(f"Total received messages: {cnt}")