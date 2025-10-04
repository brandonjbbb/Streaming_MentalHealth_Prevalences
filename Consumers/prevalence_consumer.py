# I consume prevalence events from Kafka and compute a rolling average for a quick, live stability read.

import os, json
from collections import deque
from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "mental-health-prevalence")

# Use a stable group id so offsets persist; start at earliest if no offsets yet.
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id="mh-prevalence-dev",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

window = 5
buf = deque(maxlen=window)

print("Listening for messages...")
for msg in consumer:
    v = msg.value
    buf.append(float(v["prevalence"]))
    avg = sum(buf) / len(buf)
    print(f"{v['timestamp']} {v['state']} {v['condition']}={v['prevalence']:.3f} | rolling_avg({len(buf)})={avg:.3f}")
