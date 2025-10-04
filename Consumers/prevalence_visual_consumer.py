# I render a live chart of mental-health prevalence as events stream in.
# The plot shows the raw value and a rolling average to smooth short-term noise.

import os, json
from collections import deque
from datetime import datetime
from kafka import KafkaConsumer

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.animation import FuncAnimation

# I keep runtime knobs in .env so anyone can retarget Kafka or tweak behavior.
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "mental-health-prevalence")
ROLLING_WINDOW = int(os.getenv("ROLLING_WINDOW", "5"))
MAX_POINTS = int(os.getenv("MAX_POINTS", "120"))  # limit visible history on the chart

# I deserialize JSON values so downstream logic can treat them as dicts.
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mh-prevalence-visual",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

# I retain the last N values to compute a rolling mean efficiently.
buf = deque(maxlen=ROLLING_WINDOW)

# I store time-series for plotting (trimmed for performance/clarity).
ts, vals, avgs = [], [], []

# I lazily fill the dynamic chart title with condition/state once the first event arrives.
title_suffix = {"state": None, "condition": None}

fig, ax = plt.subplots(figsize=(9, 5))
raw_line, = ax.plot([], [], linewidth=1.5, label="prevalence")
avg_line, = ax.plot([], [], linewidth=2.0, linestyle="--", label=f"rolling avg ({ROLLING_WINDOW})")
latest_txt = ax.text(0.01, 0.97, "", transform=ax.transAxes, va="top", ha="left")

ax.set_xlabel("time (UTC)")
ax.set_ylabel("prevalence")
ax.grid(True, alpha=0.3)
ax.legend(loc="upper left")

ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
fig.tight_layout()

def iso_to_dt(s: str) -> datetime:
    # I parse ISO 8601 timestamps like 2025-10-03T18:50:00Z into timezone-aware UTC datetimes.
    if s.endswith("Z"):
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    # fallback: naive parse if no Z
    return datetime.fromisoformat(s)

def poll_new_records():
    # I use poll() in short bursts to integrate with Matplotlib's event loop.
    records = consumer.poll(timeout_ms=100, max_records=100)
    updated = False
    for _tp, batch in records.items():
        for record in batch:
            v = record.value
            try:
                p = float(v["prevalence"])
                t = iso_to_dt(v["timestamp"])
            except Exception:
                # If a bad record sneaks in, I skip it to keep the visual robust.
                continue

            if title_suffix["state"] is None:
                title_suffix["state"] = v.get("state", "?")
            if title_suffix["condition"] is None:
                title_suffix["condition"] = v.get("condition", "?")

            buf.append(p)
            avg = sum(buf) / len(buf)

            ts.append(t)
            vals.append(p)
            avgs.append(avg)

            # I keep the arrays bounded so the chart remains responsive.
            if len(ts) > MAX_POINTS:
                del ts[: len(ts) - MAX_POINTS]
                del vals[: len(vals) - MAX_POINTS]
                del avgs[: len(avgs) - MAX_POINTS]

            updated = True
    return updated

def update(_frame):
    updated = poll_new_records()
    if not updated:
        # Nothing new; return artists for blitting without redrawing axes.
        return raw_line, avg_line, latest_txt

    # I update the plotted data.
    raw_line.set_data(ts, vals)
    avg_line.set_data(ts, avgs)

    # I keep view focused on the latest window of data.
    if ts:
        ax.set_xlim(ts[0], ts[-1])
        ymin = min(min(vals), min(avgs)) if avgs else min(vals)
        ymax = max(max(vals), max(avgs)) if avgs else max(vals)
        pad = max(0.02, (ymax - ymin) * 0.15)
        ax.set_ylim(ymin - pad, ymax + pad)

        latest_txt.set_text(
            f"latest: {vals[-1]:.3f} | rolling({len(buf)}): {avgs[-1]:.3f}"
        )

    # I keep the chart title informative once metadata is available.
    cond = title_suffix['condition'] or "condition"
    st = title_suffix['state'] or "state"
    ax.set_title(f"Mental-health prevalence over time — {cond.upper()} in {st}")

    return raw_line, avg_line, latest_txt

ani = FuncAnimation(fig, update, interval=300, blit=True)
print("visual consumer: listening and animating… (close the window to exit)")
try:
    plt.show()
finally:
    consumer.close()
