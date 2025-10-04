# I animate a live line chart of prevalence with a rolling average overlay,
# consuming JSON events from Kafka in real time.

import os, json, datetime
from collections import deque

import matplotlib
# Prefer the macOS interactive backend; fall back to TkAgg if needed.
try:
    matplotlib.use("MacOSX")
except Exception:
    matplotlib.use("TkAgg")

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from kafka import KafkaConsumer

# Externalized config via .env for easy reuse.
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "mental-health-prevalence")
WINDOW = int(os.getenv("ROLLING_WINDOW", "5"))
SPIKE_THRESH = float(os.getenv("SPIKE_THRESH", "0.03"))

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mh-prevalence-visual",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

vals = deque(maxlen=WINDOW)
xs, ys, avgs = [], [], []
t0 = None
last_v = None

def ts_to_seconds(s: str) -> float:
    """Map ISO time to seconds since first event for a clean x-axis."""
    global t0
    dt = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
    if t0 is None:
        t0 = dt
    return (dt - t0).total_seconds()

fig, ax = plt.subplots()
line_raw, = ax.plot([], [], label="prevalence")
line_avg, = ax.plot([], [], linestyle="--", label=f"rolling avg ({WINDOW})")
ax.set_xlabel("seconds since start")
ax.set_ylabel("prevalence (fraction)")
ax.set_title("Live Mental-Health Prevalence (rolling average)")
ax.legend(loc="upper left")
ax.grid(True, alpha=0.3)
text_annot = ax.text(0.02, 0.95, "", transform=ax.transAxes, va="top", ha="left")

def maybe_spike_alert(v):
    global last_v
    if last_v is None:
        last_v = v
        return None
    if abs(v - last_v) >= SPIKE_THRESH:
        msg = f"spike: Δ={v-last_v:+.3f}"
        print(msg)
        last_v = v
        return msg
    last_v = v
    return None

def update(_frame):
    polled = consumer.poll(timeout_ms=100)
    new_points = 0
    for _, records in polled.items():
        for r in records:
            v = float(r.value["prevalence"])
            x = ts_to_seconds(r.value["timestamp"])
            xs.append(x); ys.append(v)
            vals.append(v)
            avgs.append(sum(vals) / len(vals))
            new_points += 1
            alert = maybe_spike_alert(v)
            if alert:
                text_annot.set_text(alert)

    if new_points == 0:
        if text_annot.get_text():
            text_annot.set_text("")
        return line_raw, line_avg, text_annot

    ax.relim(); ax.autoscale_view()
    line_raw.set_data(xs, ys)
    line_avg.set_data(xs, avgs)
    return line_raw, line_avg, text_annot

ani = FuncAnimation(fig, update, interval=250, blit=True)

print("Listening + animating… (close the window or Ctrl+C to quit)")
try:
    plt.show()
finally:
    consumer.close()
