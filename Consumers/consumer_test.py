from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "test_topic",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="test-group"
)

print("Listening for messages...")
for msg in consumer:
    print(f"Received: {msg.value}")
