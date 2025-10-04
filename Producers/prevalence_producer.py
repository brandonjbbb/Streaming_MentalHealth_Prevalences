# Producers/prevalence_producer.py
# I stream mental-health prevalence events from a CSV into Kafka as JSON.
# This minimal producer powers my live demo and feeds the rolling-average visual downstream.

import os, csv, json, time
from kafka import KafkaProducer

# I keep connection + topic + pace configurable via .env so others can tweak without code changes.
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "mental-health-prevalence")
DELAY = float(os.getenv("PRODUCER_DELAY", "1.5"))

# I serialize dicts to UTF-8 JSON so the consumer can just json.loads().
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# I iterate the sample CSV and emit one event per row to simulate a live feed.
DATA_CSV = os.getenv("DATA_CSV", "Data/sample_prevalence.csv")
with open(DATA_CSV) as f:
    for row in csv.DictReader(f):
        row["prevalence"] = float(row["prevalence"])  # ensure numeric for downstream math
        producer.send(TOPIC, row)                      # async send to Kafka
        producer.flush()                               # keep demo deterministic / easy to follow
        print("→ sent:", row)                          # lightweight audit in the terminal
        time.sleep(DELAY)                              # pacing for readability in the consumer

print("producer: done streaming sample file.")

