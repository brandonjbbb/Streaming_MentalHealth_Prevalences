# I render a live line chart per (state, condition), each with a rolling mean.
# I support filtering via env vars and let viewers toggle series by clicking the legend.

import os, json, sys, signal
from collections import deque, defaultdict
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import numpy as np

# --- Config (env-tunable) ---
BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC         = os.getenv("TOPIC", "mental-health-prevalence")
WINDOW        = int(os.getenv("ROLLING_WINDOW", "5"))
SIGMAS        = float(os.getenv("SPIKE_SIGMAS", "2.0"))
MIN_DELTA     = float(os.getenv("SPIKE_MIN_DELTA", "0.03"))
STATE_FILTER  = [s.strip() for s in os.getenv("STATE_FILTER", "*").split(",")]
COND_FILTER   = [s.strip() for s in os.getenv("CONDITION_FILTER", "*").split(",")]

def allowed(state, cond):
    s_ok = STATE_FILTER == ["*"] or state in STATE_FILTER
    c_ok = COND_FILTER == ["*"] or cond in COND_FILTER
    return s_ok and c_ok

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mh-prevalence-visual",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

# --- Plot setup ---
fig, ax = plt.subplots(figsize=(9, 5))
ax.set_title("Live Mental-Health Prevalence (click legend to toggle series, press Q to quit)")
ax.set_xlabel("event #")
ax.set_ylabel("prevalence")
ax.grid(True, alpha=0.25)

# I manage one structure per series key like "CA–anxiety"
series = {}
legend_map = {}     # map legend handle -> key for click toggling
legend_dirty = True # rebuild legend when new series appear

def make_key(state, cond):
    return f"{state}–{cond}"

def create_series(key):
    # one raw line, one rolling mean line, and a spike scatter, all initially invisible until they get data
    line, = ax.plot([], [], lw=2, label=key, visible=True)
    mean_line, = ax.plot([], [], lw=1.5, linestyle="--", alpha=0.9, visible=True)
    scatter = ax.scatter([], [], s=60, marker="o", edgecolors="none", visible=True)
    series[key] = {
        "xs": [], "ys": [], "means": [],
        "roll": deque(maxlen=WINDOW),
        "spike_x": [], "spike_y": [],
        "line": line, "mean_line": mean_line, "scatter": scatter,
    }

def rebuild_legend():
    global legend_map
    leg = ax.legend(loc="lower right", fancybox=True, framealpha=0.5)
    legend_map = {}
    # I map legend entries back to their real lines by label
    for lh in leg.legendHandles:
        label = lh.get_label()
        if label in series:
            lh.set_picker(True)
            lh.set_pickradius(6)
            legend_map[lh] = label
    fig.canvas.draw_idle()

def compute_stats(buf):
    arr = np.asarray(buf, dtype=float)
    return float(arr.mean()), float(arr.std(ddof=0))

def maybe_spike(value, mean, std, have_window):
    if not have_window:
        return False
    if std == 0:
        return abs(value - mean) >= MIN_DELTA
    z = abs((value - mean) / std)
    return z >= SIGMAS and abs(value - mean) >= MIN_DELTA

def update(_frame):
    global legend_dirty
    polled = consumer.poll(timeout_ms=100)
    got_any = False
    created = False

    for _tp, records in polled.items():
        for rec in records:
            v = rec.value
            state = v.get("state", "")
            cond  = v.get("condition", "")
            if not allowed(state, cond):
                continue

            key = make_key(state, cond)
            if key not in series:
                create_series(key)
                created = True

            s = series[key]
            val = float(v["prevalence"])

            s["xs"].append(len(s["xs"]))  # per-series event index on x-axis
            s["ys"].append(val)
            s["roll"].append(val)

            mean, std = compute_stats(s["roll"])
            s["means"].append(mean)

            if maybe_spike(val, mean, std, len(s["roll"]) >= WINDOW):
                s["spike_x"].append(s["xs"][-1])
                s["spike_y"].append(val)

            # push updated data to artists
            s["line"].set_data(s["xs"], s["ys"])
            s["mean_line"].set_data(s["xs"], s["means"])
            if s["spike_x"]:
                offs = np.column_stack([s["spike_x"], s["spike_y"]])
                s["scatter"].set_offsets(offs)

            got_any = True

    if created:
        rebuild_legend()

    if got_any:
        ax.relim()
        ax.autoscale_view()

    # return artists for blitting
    artists = []
    for s in series.values():
        artists.extend([s["line"], s["mean_line"], s["scatter"]])
    return artists

def on_pick(event):
    # click legend line to toggle its series (raw+mean+spikes together)
    handle = event.artist
    key = legend_map.get(handle)
    if not key:
        return
    s = series[key]
    vis = not s["line"].get_visible()
    s["line"].set_visible(vis)
    s["mean_line"].set_visible(vis)
    s["scatter"].set_visible(vis)
    fig.canvas.draw_idle()

def on_key(event):
    if event.key and event.key.lower() == "q":
        _close()

def _close(*_args):
    plt.close("all")
    try:
        consumer.close()
    except Exception:
        pass
    sys.exit(0)

fig.canvas.mpl_connect("pick_event", on_pick)
fig.canvas.mpl_connect("key_press_event", on_key)
signal.signal(signal.SIGINT, _close)
signal.signal(signal.SIGTERM, _close)

ani = FuncAnimation(fig, update, interval=300, blit=True)
plt.tight_layout()
plt.show()
