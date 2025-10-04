# I read the spike CSV and emit a compact per-(state, condition) summary CSV and stdout table.

import os, csv, math, collections

SINK_PATH = os.getenv("SPIKE_SINK_PATH", "Data/spikes.csv")
OUT_PATH  = os.getenv("SPIKE_SUMMARY_OUT", "Data/spikes_summary.csv")

if not os.path.exists(SINK_PATH) or os.path.getsize(SINK_PATH) == 0:
    print(f"No spike file found or empty: {SINK_PATH}")
    raise SystemExit(0)

stats = collections.defaultdict(lambda: {
    "count": 0, "z_sum": 0.0, "z_max_abs": 0.0,
    "first_ts": None, "last_ts": None
})

with open(SINK_PATH, newline="") as f:
    for row in csv.DictReader(f):
        state, cond = row["state"], row["condition"]
        key = (state, cond)
        z = float(row.get("zscore","0") or 0)
        ts = row.get("timestamp","")
        s = stats[key]
        s["count"] += 1
        s["z_sum"] += abs(z)
        s["z_max_abs"] = max(s["z_max_abs"], abs(z))
        s["first_ts"] = ts if s["first_ts"] is None else s["first_ts"]
        s["last_ts"]  = ts  # last seen

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
with open(OUT_PATH, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["state","condition","spikes","avg_abs_z","max_abs_z","first_ts","last_ts"])
    for (state, cond), s in sorted(stats.items()):
        avg_abs_z = (s["z_sum"]/s["count"]) if s["count"] else 0.0
        w.writerow([state, cond, s["count"], f"{avg_abs_z:.3f}", f"{s['z_max_abs']:.3f}", s["first_ts"], s["last_ts"]])

print("Summary (spikes per (state, condition))")
print("state,condition,spikes,avg_abs_z,max_abs_z,first_ts,last_ts")
with open(OUT_PATH) as f:
    next(f)  # skip header we just printed
    for line in f:
        print(line.strip())
