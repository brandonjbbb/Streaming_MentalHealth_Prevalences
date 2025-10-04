# Consumers/prevalence_live_anomaly.py
# I visualize a live stream, flag anomalies via rolling z-score, and auto-save PNG snapshots for the README.

import os, json, threading, time
from collections import deque
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from math import sqrt

# Config via .env
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "mental-health-prevalence")
WINDOW = int(os.getenv("ROLLING_WINDOW", "5"))             # rolling window
ANOM_Z = float(os.getenv("ANOM_Z", "2.0"))                 # anomaly threshold
ALERT_MIN = int(os.getenv("ALERT_MIN_POINTS", "5"))        # minimum points before flagging
MAX_POINTS = int(os.getenv("MAX_POINTS", "100"))           # cap history for redraws
SNAPSHOT_SEC = float(os.getenv("SNAPSHOT_EVERY_SEC", "5")) # 0 disables autosave
SNAPSHOT_PATH = os.getenv("SNAPSHOT_PATH", "images/live_prevalence.png")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mh-prevalence-live-anom",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

# Shared state
xs, ys, avgs = [], [], []
anom_x, anom_y = [], []
roll = deque(maxlen=WINDOW)

def consume_loop():
    idx = 0
    for msg in consumer:
        v = msg.value
        p = float(v["prevalence"])
        roll.append(p); idx += 1

        mean = sum(roll) / len(roll)
        std = sqrt(sum((x - mean) ** 2 for x in roll) / (len(roll) - 1)) if len(roll) > 1 else 0.0
        z = (p - mean) / std if std > 0 else 0.0
        flagged = len(roll) >= ALERT_MIN and abs(z) >= ANOM_Z

        xs.append(idx); ys.append(p); avgs.append(mean)
        if flagged:
            anom_x.append(idx); anom_y.append(p)

        if len(xs) > MAX_POINTS:
            del xs[0], ys[0], avgs[0]
        if len(anom_x) > MAX_POINTS:
            del anom_x[0], anom_y[0]

        tag = " ⚠️ ANOM" if flagged else ""
        print(f"{v['timestamp']} {v['state']} {v['condition']}={p:.3f} | "
              f"roll({len(roll)}): mean={mean:.3f} std={std:.3f} z={z:.2f}{tag}")

t = threading.Thread(target=consume_loop, daemon=True)
t.start()

# Matplotlib figure
fig, ax = plt.subplots(figsize=(9, 4.8))
raw_line, = ax.plot([], [], label="prevalence")
avg_line, = ax.plot([], [], label=f"rolling avg ({WINDOW})")
anom_pts, = ax.plot([], [], "o", label=f"anomaly | |z|≥{ANOM_Z}", linestyle="None")

ax.set_title("Live Mental-Health Prevalence — Rolling Baseline & Anomaly Flags")
ax.set_xlabel("event #"); ax.set_ylabel("prevalence (fraction)")
ax.legend(loc="upper left"); ax.grid(True, alpha=0.3)

caption = ax.text(
    0.01, 0.97, "", transform=ax.transAxes, va="top", ha="left",
    bbox=dict(boxstyle="round", alpha=0.15, pad=0.3)
)

_last_snap = time.monotonic()

def update(_frame):
    global _last_snap
    if not xs:
        return raw_line, avg_line, anom_pts, caption

    ax.set_xlim(max(0, xs[0]-1), xs[-1] + 1)
    y_all = ys + avgs + (anom_y if anom_y else [])
    ymin, ymax = (min(y_all), max(y_all)) if y_all else (0.0, 1.0)
    pad = max(0.02, 0.12 * (ymax - ymin) if ymax > ymin else 0.05)
    ax.set_ylim(ymin - pad, ymax + pad)

    raw_line.set_data(xs, ys)
    avg_line.set_data(xs, avgs)
    anom_pts.set_data(anom_x, anom_y)

    flag = "⚠️ anomaly" if (anom_x and anom_x[-1] == xs[-1]) else "—"
    caption.set_text(
        f"last: {ys[-1]:.3f}  |  rolling mean: {avgs[-1]:.3f}  |  window: {WINDOW}\n"
        f"flag: {flag} (threshold |z| ≥ {ANOM_Z})"
    )

    # periodic snapshot for README
    if SNAPSHOT_SEC > 0 and (time.monotonic() - _last_snap) >= SNAPSHOT_SEC:
        os.makedirs(os.path.dirname(SNAPSHOT_PATH), exist_ok=True)
        fig.savefig(SNAPSHOT_PATH, dpi=150)
        _last_snap = time.monotonic()

    return raw_line, avg_line, anom_pts, caption

ani = FuncAnimation(fig, update, interval=500, blit=False)
plt.tight_layout()
plt.show()
