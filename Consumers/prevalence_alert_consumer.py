# I visualize the stream and flag spikes when prevalence jumps above a 2σ band.
# I also append each spike to Data/alerts.csv for quick post-run review.

import os, json, csv, os.path
from collections import deque
from datetime import datetime
import numpy as np
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

# I read connection + topic from .env for portability.
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "mental-health-prevalence")

# I keep a short window for "feel" and compute mu/σ over it.
WINDOW = int(os.getenv("ROLLING_WINDOW", "5"))
Z_THRESHOLD = float(os.getenv("Z_THRESHOLD", "2.0"))  # 2σ rule-of-thumb

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mh-prevalence-visual-alerts",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

# I keep time/value history for the animated line; spikes get a separate scatter layer.
idx = 0
x_vals, y_vals = [], []
spike_x, spike_y = [], []
win = deque(maxlen=WINDOW)

# I ensure the alerts CSV exists with a header.
os.makedirs("Data", exist_ok=True)
alerts_path = "Data/alerts.csv"
if not os.path.exists(alerts_path):
    with open(alerts_path, "w", newline="") as f:
        csv.writer(f).writerow(["ts", "state", "condition", "value", "mean", "std", "z"])

# I set up a simple animated line with a faint rolling band for context.
plt.figure(figsize=(8, 4.5))
line, = plt.plot([], [], lw=2, label="prevalence")
spike_scatter = plt.scatter([], [], s=60, marker="o", label="spike (z>2)")
band_upper, = plt.plot([], [], linestyle="--", alpha=0.5, label="mean+2σ")
band_lower, = plt.plot([], [], linestyle="--", alpha=0.5, label="mean-2σ")
plt.title("Live Prevalence with Spike Alerts")
plt.xlabel("event #")
plt.ylabel("prevalence")
plt.legend(loc="upper left")
plt.grid(True, alpha=0.2)

# I buffer records from Kafka and let FuncAnimation pull from this buffer.
inbox = deque()

def fetch_messages(max_batch=50):
    """I pull a small batch from Kafka to keep UI responsive."""
    polled = consumer.poll(timeout_ms=300)
    count = 0
    for records in polled.values():
        for r in records:
            inbox.append(r.value)
            count += 1
            if count >= max_batch:
                return

def on_frame(_):
    global idx
    # pull fresh messages if the inbox is low
    if len(inbox) < 2:
        fetch_messages()

    if not inbox:
        # nothing new; keep drawing last frame
        return line,

    msg = inbox.popleft()
    v = float(msg["prevalence"])
    win.append(v)

    idx += 1
    x_vals.append(idx)
    y_vals.append(v)

    # rolling stats once we have >1 point
    mu = float(np.mean(win)) if len(win) > 0 else v
    sigma = float(np.std(win, ddof=0)) if len(win) > 1 else 0.0
    z = (v - mu) / (sigma if sigma > 1e-9 else 1.0)

    # I flag & annotate spikes
    if sigma > 0 and z > Z_THRESHOLD:
        spike_x.append(idx)
        spike_y.append(v)
        with open(alerts_path, "a", newline="") as f:
            csv.writer(f).writerow([
                msg["timestamp"], msg["state"], msg["condition"], f"{v:.4f}", f"{mu:.4f}", f"{sigma:.4f}", f"{z:.3f}"
            ])
        print(f"ALERT z>{Z_THRESHOLD}: {msg['timestamp']} {msg['state']} {msg['condition']}={v:.3f} "
              f"(μ={mu:.3f}, σ={sigma:.3f}, z={z:.2f})")

    # update the line and bands
    line.set_data(x_vals, y_vals)
    band_upper.set_data(x_vals, [float(np.mean(y_vals[max(0, i-WINDOW+1):i+1])) +
                                 2*float(np.std(y_vals[max(0, i-WINDOW+1):i+1], ddof=0)) if i>0 else y
                                 for i, y in enumerate(y_vals)])
    band_lower.set_data(x_vals, [float(np.mean(y_vals[max(0, i-WINDOW+1):i+1])) -
                                 2*float(np.std(y_vals[max(0, i-WINDOW+1):i+1], ddof=0)) if i>0 else y
                                 for i, y in enumerate(y_vals)])

    # update spikes
    spike_scatter.set_offsets(np.c_[spike_x, spike_y])

    # keep a comfortable x-span
    plt.xlim(max(0, idx - 30), idx + 1)
    # y-span with a touch of padding
    ymin = min(y_vals) if y_vals else 0.0
    ymax = max(y_vals) if y_vals else 1.0
    pad = max(0.02, 0.1 * (ymax - ymin))
    plt.ylim(ymin - pad, ymax + pad)

    return line, spike_scatter, band_upper, band_lower

ani = FuncAnimation(plt.gcf(), on_frame, interval=500, blit=False)
plt.tight_layout()
print("Listening for messages with spike alerts...")
try:
    plt.show()
finally:
    consumer.close()
