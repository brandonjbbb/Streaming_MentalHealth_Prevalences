from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "test_topic"

for i in range(5):
    message = {"msg": f"hello world {i}"}
    producer.send(topic, value=message)
    print(f"Sent: {message}")
    time.sleep(1)

producer.flush()

