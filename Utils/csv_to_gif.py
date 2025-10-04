# I turn a small CSV stream into an animated GIF of prevalence vs a rolling average.
# This is a deterministic way to capture the visual for the README without screen recording.

import argparse, csv, math, os
from datetime import datetime
from collections import deque

import matplotlib
matplotlib.use("Agg")  # headless: render without opening a window
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter

def parse_args():
    p = argparse.ArgumentParser(description="Render prevalence CSV to animated GIF")
    p.add_argument("csv_path", help="Input CSV like Data/sample_prevalence.csv")
    p.add_argument("gif_out", help="Output GIF path, e.g., images/demo.gif")
    p.add_argument("--fps", type=int, default=6, help="Frames per second (default: 6)")
    p.add_argument("--window", type=int, default=5, help="Rolling average window (default: 5)")
    return p.parse_args()

def read_rows(csv_path):
    rows = []
    with open(csv_path) as f:
        for r in csv.DictReader(f):
            r["prevalence"] = float(r["prevalence"])
            rows.append(r)
    return rows

def main():
    args = parse_args()
    rows = read_rows(args.csv_path)
    os.makedirs(os.path.dirname(args.gif_out), exist_ok=True)

    xs, ys = [], []
    avg_buf = deque(maxlen=args.window)
    avg_vals = []

    fig, ax = plt.subplots(figsize=(7.2, 4.2), dpi=120)
    ax.set_title("Mental Health Prevalence (CSV replay → GIF)")
    ax.set_xlabel("Event index")
    ax.set_ylabel("Prevalence")
    ax.grid(True, alpha=0.25)

    (line_raw,) = ax.plot([], [], label="point prevalence", lw=1.75)
    (line_avg,) = ax.plot([], [], label=f"rolling avg (w={args.window})", lw=2.5)
    ax.legend(loc="upper left")

    def init():
        ax.set_xlim(0, max(10, len(rows)))
        ax.set_ylim(0, max([r["prevalence"] for r in rows] + [0.3]) * 1.15)
        return (line_raw, line_avg)

    def update(i):
        r = rows[i]
        xs.append(i)
        ys.append(r["prevalence"])

        avg_buf.append(r["prevalence"])
        avg_vals.append(sum(avg_buf) / len(avg_buf))

        line_raw.set_data(xs, ys)
        line_avg.set_data(xs, avg_vals)

        ax.set_xlim(0, max(10, len(xs)))
        ax.set_ylim(0, max(max(ys), 0.01) * 1.15)

        ax.set_title(
            f"Prevalence (state={r['state']} cond={r['condition']})  "
            f"t={r['timestamp']}  last={r['prevalence']:.3f}  "
            f"avg({len(avg_buf)})={avg_vals[-1]:.3f}"
        )
        return (line_raw, line_avg)

    anim = FuncAnimation(
        fig, update, frames=range(len(rows)), init_func=init, blit=True, interval=1000/args.fps
    )

    writer = PillowWriter(fps=args.fps)
    anim.save(args.gif_out, writer=writer)
    print(f"wrote GIF → {args.gif_out}")

if __name__ == "__main__":
    main()
