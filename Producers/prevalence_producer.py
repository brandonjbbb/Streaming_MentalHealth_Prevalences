# Producers/prevalence_producer.py
# Streams mental-health prevalence events from CSV into Kafka as JSON.

import os, csv, json, time
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC     = os.getenv("TOPIC", "mental-health-prevalence")
DELAY     = float(os.getenv("PRODUCER_DELAY", "1.5"))
DATA_CSV  = os.getenv("DATA_CSV", "Data/prevalence_nhis_2019.csv")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

with open(DATA_CSV) as f:
    for row in csv.DictReader(f):
        try:
            row["prevalence"] = float(row["prevalence"])
        except Exception:
            continue  # skip bad rows
        producer.send(TOPIC, row)
        producer.flush()
        print("→ sent:", row)
        time.sleep(DELAY)

print(f"producer: finished streaming {DATA_CSV}.")
