# Consumers/prevalence_live.py
# I render a live Matplotlib chart while consuming Kafka events.
# The plot shows the raw prevalence and a rolling average to surface trend.

import os, json, threading, time
from collections import deque
from kafka import KafkaConsumer

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

# Config via .env so the same file runs on any box with minimal edits
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "mental-health-prevalence")
WINDOW = int(os.getenv("ROLLING_WINDOW", "5"))      # rolling avg window
MAX_POINTS = int(os.getenv("MAX_POINTS", "100"))    # keep chart snappy

# Kafka consumer: earliest so the demo can replay; JSON payloads
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mh-prevalence-live",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

# Shared buffers the animation reads from
xs, ys, avgs = [], [], []
roll = deque(maxlen=WINDOW)

# Background thread to pull messages continuously
def consume_loop():
    idx = 0
    for msg in consumer:
        v = msg.value
        p = float(v["prevalence"])
        roll.append(p)
        idx += 1

        # cap history for performance/clarity
        xs.append(idx)
        ys.append(p)
        avgs.append(sum(roll) / len(roll))
        if len(xs) > MAX_POINTS:
            del xs[0], ys[0], avgs[0]

        # lightweight log so the terminal still tells the story
        print(f"{v['timestamp']} {v['state']} {v['condition']}={p:.3f} | roll({len(roll)})={avgs[-1]:.3f}")

t = threading.Thread(target=consume_loop, daemon=True)
t.start()

# Figure/axes setup
fig, ax = plt.subplots(figsize=(8, 4.5))
raw_line, = ax.plot([], [], label="prevalence")
avg_line, = ax.plot([], [], label=f"rolling avg ({WINDOW})")
ax.set_title("Live Mental-Health Prevalence (demo stream)")
ax.set_xlabel("event #")
ax.set_ylabel("prevalence (fraction)")
ax.legend(loc="upper left")
ax.grid(True, alpha=0.3)

# Matplotlib animation callback
def update(_frame):
    if not xs:
        return raw_line, avg_line
    ax.set_xlim(max(0, xs[0]-1), xs[-1] + 1)
    y_all = ys + avgs
    ymin = min(y_all) if y_all else 0.0
    ymax = max(y_all) if y_all else 1.0
    pad = max(0.02, 0.1 * (ymax - ymin))  # gentle padding
    ax.set_ylim(ymin - pad, ymax + pad)

    raw_line.set_data(xs, ys)
    avg_line.set_data(xs, avgs)
    return raw_line, avg_line

ani = FuncAnimation(fig, update, interval=500, blit=False)
plt.tight_layout()
plt.show()
