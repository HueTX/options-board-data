#!/usr/bin/env python3
"""Build follow_through.json for the standalone options board.

Source of truth: the scanner's own records on this machine —
  history.csv      (date, ticker, options_volume — banked daily)
  baselines_90d.csv (symbol, avg90_volume — the same baseline build_snapshot_payload.py uses)

Ignition criterion: options relativeVolume >= 3.0, identical to the
scanner's `ignited` flag in build_snapshot_payload.py (and the live board).

Output: follow_through.json with the most recent completed trading day's
ignitions, e.g.:
  {"asOf": "...", "date": "2026-09-28",
   "ignitions": [{"ticker": "FAZ", "x": 9.3}, ...]}

"Yesterday" = the latest date in history.csv strictly before today.
If history has no prior day (or no ignitions), the file carries an empty
ignitions list and the page shows an honest empty state — values are never
fabricated.

Usage:
  python3 build_follow_through.py [out_path]
Default out_path: ~/workspace/your_files/options-board-html/follow_through.json
"""
import csv, json, os, sys
from datetime import date

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser(
    "~/workspace/your_files/options-board-html/follow_through.json")

IGNITE_LEVEL = 3.0

baselines = {}
with open(os.path.join(BASE, "baselines_90d.csv")) as f:
    for r in csv.DictReader(f):
        try:
            baselines[r["symbol"].strip().upper()] = float(r["avg90_volume"])
        except (KeyError, ValueError):
            pass

vol = {}
with open(os.path.join(BASE, "history.csv")) as f:
    for r in csv.DictReader(f):
        try:
            vol.setdefault(r["date"], {})[r["ticker"].strip().upper()] = float(r["options_volume"])
        except (KeyError, ValueError):
            pass

today = date.today().isoformat()
prior = sorted(d for d in vol if d < today)
day = prior[-1] if prior else None

ignitions = []
if day:
    for t, v in vol[day].items():
        b = baselines.get(t)
        if b and b > 0:
            rv = v / b
            if rv >= IGNITE_LEVEL:
                ignitions.append({"ticker": t, "x": round(rv, 1)})
    ignitions.sort(key=lambda e: -e["x"])

payload = {
    "asOf": date.today().isoformat() + "T00:00:00Z",
    "date": day,
    "ignitions": ignitions,
}
with open(OUT, "w") as f:
    json.dump(payload, f, indent=2)
print(f"wrote {OUT}: date={day} ignitions={len(ignitions)}")
