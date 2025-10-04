#!/usr/bin/env python3
"""
Normalize a CDC Household Pulse Survey export to my streaming schema:
input:  CDC HPS export (CSV)
output: timestamp,state,condition,prevalence (prevalence in 0..1)

Usage:
  python3 Utils/prepare_hps_to_csv.py Data/hps_raw.csv Data/prevalence_hps_CA_anxiety.csv --state CA --condition anxiety
"""
import csv, sys, argparse, datetime as dt
from pathlib import Path

def sniff(colnames, candidates):
    lower = {c.lower(): c for c in colnames}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None

def parse_date(s):
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(s, fmt)
        except ValueError:
            pass
    # try first 10 chars (YYYY-MM-DD…)
    try:
        return dt.datetime.fromisoformat(s[:10])
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("in_csv")
    ap.add_argument("out_csv")
    ap.add_argument("--state", required=True, help="2-letter state code, e.g., CA")
    ap.add_argument("--condition", required=True, help="e.g., anxiety or depression")
    args = ap.parse_args()

    in_path = Path(args.in_csv)
    out_path = Path(args.out_csv)
    if not in_path.exists():
        sys.exit(f"ERR: input not found: {in_path}")

    rows = []
    with in_path.open() as f:
        rdr = csv.DictReader(f)
        cols = rdr.fieldnames or []

        # Heuristics for CDC HPS exports
        c_state = sniff(cols, ["state", "State", "State/Territory"])
        c_indicator = sniff(cols, ["Indicator", "indicator", "measure", "Measure"])
        c_value = sniff(cols, ["Value", "value", "Estimate", "Percent", "Percentage"])
        c_time = sniff(cols, [
            "Time Period Start Date", "time", "Time", "Date", "Week Start", "Start date",
            "Time Period", "Time_Period_Start_Date"
        ])

        if not all([c_state, c_value]):
            sys.exit(f"ERR: could not find needed columns in {cols}")

        for r in rdr:
            st = (r.get(c_state) or "").strip()
            if st.upper() != args.state.upper():
                continue

            # If an indicator column exists, try to keep only the requested condition
            if c_indicator:
                indicator = (r.get(c_indicator) or "").lower()
                want = args.condition.lower()
                if want not in indicator:
                    # allow common wording matches (e.g., “anxiety disorder”, “felt anxious”)
                    if not any(k in indicator for k in [want, "anxiet", "depress"]):
                        continue

            # Parse numeric value; percent -> proportion
            raw_val = (r.get(c_value) or "").strip().replace("%", "")
            try:
                v = float(raw_val)
            except ValueError:
                continue
            prevalence = v / 100.0 if v > 1.0 else v

            # Time
            ts = None
            if c_time:
                ts = parse_date(r.get(c_time, ""))
            if ts is None:
                # Fall back to today if no time present
                ts = dt.datetime.utcnow()
            iso = ts.replace(microsecond=0).isoformat() + "Z"

            rows.append({
                "timestamp": iso,
                "state": args.state.upper(),
                "condition": args.condition.lower(),
                "prevalence": f"{prevalence:.6f}",
            })

    # Sort by time, write out
    rows.sort(key=lambda x: x["timestamp"])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp","state","condition","prevalence"])
        w.writeheader()
        w.writerows(rows)

    print(f"wrote {len(rows)} rows -> {out_path}")

if __name__ == "__main__":
    main()
