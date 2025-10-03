from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for i in range(5):
    message = {"number": i, "status": "ok"}
    producer.send("test_topic", message)
    print(f"Sent: {message}")
    time.sleep(1)

producer.flush()
producer.close()
