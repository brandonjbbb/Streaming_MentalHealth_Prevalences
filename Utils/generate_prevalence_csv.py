# I generate a demo CSV with multiple (state, condition) series evolving as random walks.

import argparse, csv, random, datetime as dt, os

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=300)
    ap.add_argument("--states", type=str, default="CA,NY,TX")
    ap.add_argument("--conditions", type=str, default="anxiety,dep")
    ap.add_argument("--out", type=str, default="Data/sample_prevalence.csv")
    ap.add_argument("--seed", type=int, default=42)
    return ap.parse_args()

def main():
    a = parse_args()
    random.seed(a.seed)
    states = [s.strip() for s in a.states.split(",") if s.strip()]
    conds  = [s.strip() for s in a.conditions.split(",") if s.strip()]
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    start = dt.datetime.utcnow()

    # initialize each series with a base level
    base = {(s, c): random.uniform(0.15, 0.25) for s in states for c in conds}

    with open(a.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp","state","condition","prevalence"])
        for i in range(a.rows):
            ts = (start + dt.timedelta(seconds=2*i)).isoformat(timespec="seconds")+"Z"
            # pick a random series each step to simulate staggered updates
            s = random.choice(states)
            c = random.choice(conds)
            mean = base[(s,c)]
            # random walk step with slight mean reversion
            step = random.gauss(0, 0.01)
            mean = 0.9*mean + 0.1*random.uniform(0.15, 0.30) + step
            mean = max(0.05, min(0.5, mean))  # clamp to reasonable range
            base[(s,c)] = mean
            w.writerow([ts, s, c, f"{mean:.3f}"])

if __name__ == "__main__":
    main()
