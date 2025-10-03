from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "test_topic",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: v.decode("utf-8"),  # just decode to string
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="test-group"
)

print("Listening for messages...")
for msg in consumer:
    try:
        # Try JSON parse
        value = json.loads(msg.value)
        print(f"Received JSON: {value}")
    except json.JSONDecodeError:
        # Fallback: just print raw string
        print(f"Received (non-JSON): {msg.value}")
