# Consumers/prevalence_live_anomaly.py
# I visualize a live stream and flag anomalies using a rolling z-score baseline.

import os, json, threading
from collections import deque
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from math import sqrt

# Config driven via .env for reproducibility across machines
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "mental-health-prevalence")
WINDOW = int(os.getenv("ROLLING_WINDOW", "5"))            # rolling window for baseline
ANOM_Z = float(os.getenv("ANOM_Z", "2.0"))                # z-score threshold
ALERT_MIN = int(os.getenv("ALERT_MIN_POINTS", "5"))       # wait for minimum history
MAX_POINTS = int(os.getenv("MAX_POINTS", "100"))          # cap history for smooth redraws

# Kafka: earliest so the run is repeatable for demo purposes
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mh-prevalence-live-anom",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

# Shared state feeding the animation
xs, ys, avgs = [], [], []
anom_x, anom_y = [], []
roll = deque(maxlen=WINDOW)

def consume_loop():
    idx = 0
    for msg in consumer:
        v = msg.value
        p = float(v["prevalence"])
        roll.append(p)
        idx += 1

        # rolling stats for baseline
        mean = sum(roll) / len(roll)
        std = sqrt(sum((x - mean) ** 2 for x in roll) / (len(roll) - 1)) if len(roll) > 1 else 0.0
        z = (p - mean) / std if std > 0 else 0.0
        flagged = len(roll) >= ALERT_MIN and abs(z) >= ANOM_Z

        xs.append(idx); ys.append(p); avgs.append(mean)
        if flagged:
            anom_x.append(idx); anom_y.append(p)

        # trim history
        if len(xs) > MAX_POINTS:
            del xs[0], ys[0], avgs[0]
        if len(anom_x) > MAX_POINTS:
            del anom_x[0], anom_y[0]

        # terminal narrative
        tag = " ⚠️ ANOM" if flagged else ""
        print(f"{v['timestamp']} {v['state']} {v['condition']}={p:.3f} | "
              f"roll({len(roll)}): mean={mean:.3f} std={std:.3f} z={z:.2f}{tag}")

t = threading.Thread(target=consume_loop, daemon=True)
t.start()

# Matplotlib setup
fig, ax = plt.subplots(figsize=(9, 4.8))
raw_line, = ax.plot([], [], label="prevalence")
avg_line, = ax.plot([], [], label=f"rolling avg ({WINDOW})")
anom_pts, = ax.plot([], [], "o", label=f"anomaly | |z|≥{ANOM_Z}", linestyle="None")

ax.set_title("Live Mental-Health Prevalence — Rolling Baseline & Anomaly Flags")
ax.set_xlabel("event #")
ax.set_ylabel("prevalence (fraction)")
ax.legend(loc="upper left")
ax.grid(True, alpha=0.3)

# caption overlay (auto-updated)
caption = ax.text(
    0.01, 0.97, "", transform=ax.transAxes, va="top", ha="left",
    bbox=dict(boxstyle="round", alpha=0.15, pad=0.3)
)

def update(_frame):
    if not xs:
        return raw_line, avg_line, anom_pts, caption

    # autoscale with a little breathing room
    ax.set_xlim(max(0, xs[0]-1), xs[-1] + 1)
    y_all = (ys + avgs + (anom_y if anom_y else []))
    ymin, ymax = (min(y_all), max(y_all)) if y_all else (0.0, 1.0)
    pad = max(0.02, 0.12 * (ymax - ymin) if ymax > ymin else 0.05)
    ax.set_ylim(ymin - pad, ymax + pad)

    raw_line.set_data(xs, ys)
    avg_line.set_data(xs, avgs)
    anom_pts.set_data(anom_x, anom_y)

    # caption uses the latest values
    last = ys[-1]
    last_avg = avgs[-1]
    last_std = (abs(last - last_avg)) if len(ys) == 1 else None  # placeholder for initial frame
    flag = "⚠️ anomaly" if (anom_x and anom_x[-1] == xs[-1]) else "—"
    caption.set_text(
        f"last: {last:.3f}  |  rolling mean: {last_avg:.3f}  |  window: {WINDOW}\n"
        f"flag: {flag} (threshold |z| ≥ {ANOM_Z})"
    )
    return raw_line, avg_line, anom_pts, caption

ani = FuncAnimation(fig, update, interval=500, blit=False)
plt.tight_layout()
plt.show()
