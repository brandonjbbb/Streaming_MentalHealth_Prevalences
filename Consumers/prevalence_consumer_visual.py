# I render a live line chart of prevalence and a rolling mean, and flag spikes in real time.

import os, json, sys, signal
from collections import deque
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import numpy as np

# I keep these configurable so others can tune sensitivity without code edits.
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC      = os.getenv("TOPIC", "mental-health-prevalence")
WINDOW     = int(os.getenv("ROLLING_WINDOW", "5"))          # rolling mean window
SIGMAS     = float(os.getenv("SPIKE_SIGMAS", "2.0"))        # spike threshold (z-score)
MIN_DELTA  = float(os.getenv("SPIKE_MIN_DELTA", "0.03"))    # minimum absolute move to count as spike

# I create a consumer that reads earliest so the demo is deterministic.
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mh-prevalence-visual",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

# I maintain series buffers for plotting and spike markers.
xs, ys, means = [], [], []
roll = deque(maxlen=WINDOW)
spike_x, spike_y = [], []

# I set up the figure once; FuncAnimation only updates data.
fig, ax = plt.subplots(figsize=(8, 4.8))
(line,) = ax.plot([], [], lw=2, label="prevalence")
(mean_line,) = ax.plot([], [], lw=2, linestyle="--", label=f"rolling mean (w={WINDOW})")
spike_scatter = ax.scatter([], [], s=60, marker="o", edgecolors="none", label="spike")
alert_txt = ax.text(
    0.01, 0.98, "", transform=ax.transAxes, va="top", ha="left", fontsize=10
)

ax.set_title("Live Mental-Health Prevalence (streaming)")
ax.set_xlabel("event #")
ax.set_ylabel("prevalence")
ax.grid(True, alpha=0.25)
ax.legend(loc="lower right")

def compute_stats(buf):
    # I compute mean/std for the current rolling buffer.
    arr = np.array(buf, dtype=float)
    mean = float(arr.mean())
    std = float(arr.std(ddof=0))
    return mean, std

def maybe_spike(value, mean, std):
    # I require both a z-score threshold and a minimum absolute movement to reduce false positives.
    if len(roll) < WINDOW:
        return False
    if std == 0:
        return abs(value - mean) >= MIN_DELTA
    z = abs((value - mean) / std)
    return z >= SIGMAS and abs(value - mean) >= MIN_DELTA

def update(_frame):
    # I poll Kafka briefly so the UI stays responsive.
    alerted = ""
    polled = consumer.poll(timeout_ms=100)
    got_any = False

    for _tp, records in polled.items():
        for rec in records:
            got_any = True
            v = float(rec.value["prevalence"])
            xs.append(len(xs))           # event index as x-axis; simple and robust
            ys.append(v)

            roll.append(v)
            m, s = compute_stats(roll)
            means.append(m)

            if maybe_spike(v, m, s):
                spike_x.append(xs[-1])
                spike_y.append(v)
                direction = "↑" if v > m else "↓"
                alerted = f"SPIKE {direction}  v={v:.3f}  μ={m:.3f}  σ={s:.3f}"

    # I update the plot only if new records arrived; still return artists for blitting.
    if got_any:
        line.set_data(xs, ys)
        mean_line.set_data(xs, means)

        if spike_x:
            # set_offsets wants an (N,2) array-like
            offs = np.column_stack([spike_x, spike_y])
            spike_scatter.set_offsets(offs)

        # I keep axes comfortable around incoming data.
        ax.relim()
        ax.autoscale_view()

        # I keep a subtle alert banner in the corner when a spike happens.
        alert_txt.set_text(alerted)

    return line, mean_line, spike_scatter, alert_txt

def _close(*_args):
    plt.close("all")
    try:
        consumer.close()
    except Exception:
        pass
    sys.exit(0)

# I handle Ctrl+C cleanly.
signal.signal(signal.SIGINT, _close)
signal.signal(signal.SIGTERM, _close)

ani = FuncAnimation(fig, update, interval=300, blit=True)
plt.tight_layout()
plt.show()
