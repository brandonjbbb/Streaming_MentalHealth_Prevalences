# I render a dual-panel live view and persist spike events to CSV when configured.
#   • Top: raw prevalence per (state, condition) + rolling mean + spike markers
#   • Bottom: corresponding z-score stream with ±threshold lines
# I keep filter knobs in .env; legend clicks toggle the whole series in both panels.

import os, os.path, json, sys, signal, csv
from collections import deque
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import numpy as np
from matplotlib import cm

# --- Config (env-tunable) ---
BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC         = os.getenv("TOPIC", "mental-health-prevalence")
WINDOW        = int(os.getenv("ROLLING_WINDOW", "5"))
SIGMAS        = float(os.getenv("SPIKE_SIGMAS", "2.0"))
MIN_DELTA     = float(os.getenv("SPIKE_MIN_DELTA", "0.03"))
STATE_FILTER  = [s.strip() for s in os.getenv("STATE_FILTER", "*").split(",")]
COND_FILTER   = [s.strip() for s in os.getenv("CONDITION_FILTER", "*").split(",")]
SINK_PATH     = os.getenv("SPIKE_SINK_PATH", "").strip()  # e.g., Data/spikes.csv

def allowed(state, cond):
    s_ok = STATE_FILTER == ["*"] or state in STATE_FILTER
    c_ok = COND_FILTER == ["*"] or cond in COND_FILTER
    return s_ok and c_ok

# --- Kafka consumer ---
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mh-prevalence-visual",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

# --- Optional CSV sink for spikes ---
spike_fp = None
spike_writer = None
if SINK_PATH:
    os.makedirs(os.path.dirname(SINK_PATH), exist_ok=True)
    header = ["event_index","timestamp","state","condition","prevalence","rolling_mean","rolling_std","zscore","window","threshold_sigmas","min_delta"]
    new_file = not os.path.exists(SINK_PATH) or os.path.getsize(SINK_PATH) == 0
    spike_fp = open(SINK_PATH, "a", newline="")
    spike_writer = csv.writer(spike_fp)
    if new_file:
        spike_writer.writerow(header)

# --- Color mapping: stable color per series key (tab20 palette) ---
def color_for_key(key: str):
    palette = cm.get_cmap("tab20").colors  # 20-tuple RGBA
    idx = (hash(key) % len(palette))
    return palette[idx]

# --- Figure layout (dual panel) ---
fig, (ax_top, ax_bot) = plt.subplots(
    2, 1, figsize=(10, 6), sharex=True,
    gridspec_kw={"height_ratios": [3, 1], "hspace": 0.12}
)
ax_top.set_title("Live Mental-Health Prevalence (legend toggles series, press Q to quit)")
ax_top.set_ylabel("prevalence")
ax_top.grid(True, alpha=0.25)

ax_bot.set_xlabel("event # (per series)")
ax_bot.set_ylabel("z-score")
ax_bot.grid(True, alpha=0.25)
ax_bot.axhline(SIGMAS, linestyle=":", linewidth=1)
ax_bot.axhline(-SIGMAS, linestyle=":", linewidth=1)

# I keep all artists + buffers per series key like "CA–anxiety".
series = {}       # key -> dict of data + artists
legend_map = {}   # legend handle -> key

def key_of(state, cond): return f"{state}–{cond}"

def create_series(key):
    c = color_for_key(key)
    # top panel: raw + rolling mean + spike markers
    (line_raw,)  = ax_top.plot([], [], lw=2, label=key, visible=True, color=c)
    (line_mean,) = ax_top.plot([], [], lw=1.5, linestyle="--", alpha=0.95, visible=True, color=c)
    sc_spikes    = ax_top.scatter([], [], s=60, marker="o", edgecolors="none", visible=True)
    sc_spikes.set_facecolor(c)

    # bottom panel: z-score line
    (line_z,) = ax_bot.plot([], [], lw=1.5, alpha=0.95, visible=True, color=c)

    series[key] = {
        # buffers
        "xs": [], "ys": [], "means": [], "zs": [],
        "roll": deque(maxlen=WINDOW),
        "spike_x": [], "spike_y": [],
        # artists
        "raw": line_raw, "mean": line_mean, "spike": sc_spikes, "z": line_z,
    }

def rebuild_legend():
    global legend_map
    leg = ax_top.legend(loc="lower right", fancybox=True, framealpha=0.5)
    legend_map = {}
    for lh in leg.legendHandles:
        label = lh.get_label()
        if label in series:
            lh.set_picker(True)
            lh.set_pickradius(6)
            legend_map[lh] = label
    fig.canvas.draw_idle()

def stats(buf):
    arr = np.asarray(buf, dtype=float)
    return float(arr.mean()), float(arr.std(ddof=0))

def is_spike(value, mean, std, have_window):
    if not have_window: return False
    if std == 0: return abs(value - mean) >= MIN_DELTA
    z = abs((value - mean) / std)
    return z >= SIGMAS and abs(value - mean) >= MIN_DELTA

def update(_frame):
    polled = consumer.poll(timeout_ms=100)
    created = False
    got_any = False

    for _tp, records in polled.items():
        for rec in records:
            v = rec.value
            state = v.get("state", "")
            cond  = v.get("condition", "")
            if not allowed(state, cond):
                continue

            k = key_of(state, cond)
            if k not in series:
                create_series(k)
                created = True

            s = series[k]
            val = float(v["prevalence"])

            # x position is per-series event index to keep lines continuous within each key
            s["xs"].append(len(s["xs"]))
            s["ys"].append(val)
            s["roll"].append(val)

            mean, std = stats(s["roll"])
            s["means"].append(mean)
            z = 0.0 if std == 0 else (val - mean) / std
            s["zs"].append(z)

            # spike detection + optional CSV sink
            has_window = len(s["roll"]) >= WINDOW
            if is_spike(val, mean, std, has_window):
                s["spike_x"].append(s["xs"][-1])
                s["spike_y"].append(val)
                if spike_writer:
                    spike_writer.writerow([
                        s["xs"][-1],
                        v.get("timestamp",""),
                        state, cond, val, mean, std, z,
                        WINDOW, SIGMAS, MIN_DELTA
                    ])
                    spike_fp.flush()

            # push to artists
            s["raw"].set_data(s["xs"], s["ys"])
            s["mean"].set_data(s["xs"], s["means"])
            s["z"].set_data(s["xs"], s["zs"])
            offs = np.column_stack([s["spike_x"], s["spike_y"]]) if s["spike_x"] else np.empty((0,2))
            s["spike"].set_offsets(offs)

            got_any = True

    if created:
        rebuild_legend()

    if got_any:
        ax_top.relim(); ax_top.autoscale_view()
        ax_bot.relim(); ax_bot.autoscale_view(scaley=True)

    artists = []
    for s in series.values():
        artists.extend([s["raw"], s["mean"], s["spike"], s["z"]])
    return artists

def on_pick(event):
    # clicking a legend item toggles its entire series across both panels
    handle = event.artist
    key = legend_map.get(handle)
    if not key: return
    s = series[key]
    vis = not s["raw"].get_visible()
    s["raw"].set_visible(vis)
    s["mean"].set_visible(vis)
    s["spike"].set_visible(vis)
    s["z"].set_visible(vis)
    fig.canvas.draw_idle()

def on_key(event):
    if event.key and event.key.lower() == "q":
        _close()

def _close(*_):
    plt.close("all")
    try: consumer.close()
    except Exception: pass
    try:
        if spike_fp: spike_fp.close()
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
