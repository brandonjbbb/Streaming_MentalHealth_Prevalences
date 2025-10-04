# Consumers/prevalence_consumer_visual.py
# Live visualization of mental-health prevalence streamed via Kafka.
# Dynamically creates a line per (state, condition).
# Rolling average and z-scores are shown; press Q to quit.

import os, json, time, math
from collections import deque
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

# === Config from .env ===
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC     = os.getenv("TOPIC", "mental-health-prevalence")
WINDOW    = int(os.getenv("ROLLING_WINDOW", "6"))
GROUP_ID  = os.getenv("GROUP_ID", f"mh-visual-{int(time.time())}")

# === Kafka Consumer ===
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=GROUP_ID,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

# === Matplotlib setup ===
plt.rcParams["figure.autolayout"] = True
fig, (ax, ax_z) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)

ax.set_title("NHIS 2019 Mental-Health Prevalence (live stream)")
ax.set_ylabel("prevalence")
ax.set_ylim(0, 1)
ax.grid(True, alpha=0.3)

ax_z.set_ylabel("z-score")
ax_z.axhline(2, ls="--", alpha=0.3)
ax_z.axhline(-2, ls="--", alpha=0.3)
ax_z.grid(True, alpha=0.3)
ax_z.set_xlabel("event index")

# === Data storage ===
series = {}  # key -> {"x": [], "y": [], "line": Line2D, "buf": deque()}

def zscore(buf):
    if len(buf) < 2:
        return 0.0
    m = sum(buf) / len(buf)
    var = sum((x - m) ** 2 for x in buf) / (len(buf) - 1)
    return (buf[-1] - m) / max(math.sqrt(var), 1e-6)

def ensure_series(key):
    if key not in series:
        (line,) = ax.plot([], [], marker="o", linewidth=1.8, alpha=0.9, label=key)
        series[key] = {"x": [], "y": [], "line": line, "buf": deque(maxlen=WINDOW)}
        ax.legend(loc="upper left", fontsize=8)

# === Update function ===
def update(frame):
    polled = consumer.poll(timeout_ms=100, max_records=50)
    updated = False

    for tp, records in polled.items():
        for rec in records:
            v = rec.value
            try:
                p = float(v["prevalence"])
                key = f"{v.get('state','?')}|{v.get('condition','?')}"
                ensure_series(key)

                entry = series[key]
                entry["x"].append(len(entry["x"]))
                entry["y"].append(p)
                entry["buf"].append(p)
                entry["line"].set_data(entry["x"], entry["y"])

                # rolling avg + z-score
                avg = sum(entry["buf"]) / len(entry["buf"])
                zs = zscore(entry["buf"])

                # print debug
                print(f"{v.get('timestamp','?')} {key}={p:.3f} | "
                      f"rolling_avg({len(entry['buf'])})={avg:.3f} z={zs:.2f}")

                # plot z-score (just latest point)
                ax_z.plot([entry["x"][-1]], [zs], marker="o", alpha=0.7)
                updated = True
            except Exception as e:
                print("[visual] skipped record:", v, "err:", e)

    if updated:
        max_x = max((len(s["x"]) for s in series.values()), default=1)
        ax.set_xlim(0, max(10, max_x))
        ax_z.set_xlim(0, max(10, max_x))

    return [entry["line"] for entry in series.values()]

# === Quit with Q ===
def on_key(e):
    if e.key and e.key.lower() == "q":
        plt.close(fig)

fig.canvas.mpl_connect("key_press_event", on_key)

ani = FuncAnimation(fig, update, interval=300, blit=True, cache_frame_data=False)

print(f"[visual] Listening on topic={TOPIC}, bootstrap={BOOTSTRAP}, group={GROUP_ID}")
print("[visual] Press Q in the chart window to quit.")

plt.show()

consumer.close()
