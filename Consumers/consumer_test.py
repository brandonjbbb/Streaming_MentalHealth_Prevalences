from kafka import KafkaConsumer
import json

# Deserializer: handles JSON, plain text, or empty messages
def safe_deserializer(v):
    if v is None:
        return None
    try:
        return json.loads(v.decode("utf-8"))  # try JSON
    except Exception:
        return v.decode("utf-8")  # fallback to plain string

# Create Kafka consumer
consumer = KafkaConsumer(
    "test_topic",
    bootstrap_servers="localhost:9092",
    value_deserializer=safe_deserializer,
    auto_offset_reset="earliest"  # read from beginning
)

print("Listening for messages...")

for msg in consumer:
    print("Received:", msg.value)
