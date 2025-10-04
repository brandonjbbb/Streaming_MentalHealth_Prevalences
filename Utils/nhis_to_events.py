# I convert an NHIS monthly CSV into the event schema my pipeline expects:
# timestamp,state,condition,prevalence
# - Auto-detects a monthly date column (e.g., "month" or "date")
# - Auto-detects the first meaningful numeric column as prevalence unless --value-col is provided
# - Normalizes percentages (e.g., 23.4 -> 0.234)
# - Emits ISO timestamps at month start (e.g., 2019-01-01T00:00:00Z)

import argparse, csv, sys
from pathlib import Path
from datetime import datetime

def guess_columns(header):
    lower = [h.lower() for h in header]
    # date-ish columns
    date_cols = []
    for cand in ["date", "month", "time", "period"]:
        if cand in lower:
            date_cols.append(header[lower.index(cand)])
    # numeric candidates (skip obvious non-numeric)
    non_numeric = set(["date","month","time","period","state","condition","year"])
    num_candidates = [h for h in header if h.lower() not in non_numeric]
    # heuristic: prefer names with mental-health-ish keywords
    keywords = ("anxiety","distress","depress","mental","psych","mood")
    prefer = [h for h in num_candidates if any(k in h.lower() for k in keywords)]
    return (date_cols[0] if date_cols else None, prefer + num_candidates)

def parse_month(value, year_fallback=None):
    v = str(value).strip()
    # Accept "2019-01", "2019/01", "Jan 2019", "January 2019", "1", "01"
    fmts = ["%Y-%m","%Y/%m","%b %Y","%B %Y","%Y-%m-%d"]
    for fmt in fmts:
        try:
            dt = datetime.strptime(v, fmt)
            return datetime(dt.year, dt.month, 1)
        except ValueError:
            pass
    # Pure month number with fallback year
    if v.isdigit() and year_fallback:
        m = int(v)
        if 1 <= m <= 12:
            return datetime(int(year_fallback), m, 1)
    # As a last resort, try coercing to int month w/ 2019
    try:
        m = int(float(v))
        if 1 <= m <= 12:
            return datetime(2019, m, 1)
    except:
        pass
    raise ValueError(f"Could not parse month/date from '{value}'")

def coerce_float(x):
    s = str(x).strip()
    if s == "" or s.lower() in {"na","nan","null"}:
        return None
    try:
        val = float(s)
        # If looks like a percentage (e.g., 23.4), convert to proportion
        if val > 1.0:
            return round(val / 100.0, 6)
        return val
    except:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input_csv")
    ap.add_argument("output_csv")
    ap.add_argument("--date-col", help="Column with the month/date (auto if omitted)")
    ap.add_argument("--value-col", help="Column with prevalence value (auto if omitted)")
    ap.add_argument("--year", help="Fallback year if month is 1..12 (e.g., 2019)")
    ap.add_argument("--state", default="US", help='State code to stamp on rows (default "US")')
    ap.add_argument("--condition", default="anxiety", help='Condition label (default "anxiety")')
    args = ap.parse_args()

    inp = Path(args.input_csv)
    out = Path(args.output_csv)
    if not inp.exists():
        sys.exit(f"Input not found: {inp}")

    with inp.open(newline="") as f, out.open("w", newline="") as g:
        r = csv.reader(f)
        header = next(r)
        if args.date_col:
            date_col = args.date_col
        else:
            date_col, numeric_prefs = guess_columns(header)
            if not date_col:
                sys.exit("Could not auto-detect a date/month column. Use --date-col.")
        if args.value_col:
            value_col = args.value_col
        else:
            # choose the first column that yields a numeric value in the first data row encountered
            numeric_prefs = guess_columns(header)[1]
            value_col = None
        # indexes
        idx = {h: i for i, h in enumerate(header)}
        if date_col not in idx:
            sys.exit(f"Date column '{date_col}' not found in CSV header: {header}")

        w = csv.writer(g)
        w.writerow(["timestamp","state","condition","prevalence"])

        first_numeric_row_seen = False
        for row in r:
            # decide value_col on first data row if still unknown
            if not args.value_col and not first_numeric_row_seen:
                for cand in numeric_prefs:
                    if cand in idx:
                        val = coerce_float(row[idx[cand]])
                        if val is not None:
                            value_col = cand
                            first_numeric_row_seen = True
                            break
                if value_col is None:
                    # keep scanning until we find something numeric
                    pass

            try:
                dt = parse_month(row[idx[date_col]], args.year)
            except Exception as e:
                # skip unparsable dates
                continue

            if value_col is None or value_col not in idx:
                # skip until we find a numeric column
                continue

            val = coerce_float(row[idx[value_col]])
            if val is None:
                continue

            ts = dt.strftime("%Y-%m-01T00:00:00Z")
            w.writerow([ts, args.state, args.condition, f"{val:.6f}"])

    print(f"wrote {out}")

if __name__ == "__main__":
    main()
