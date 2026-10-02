#!/usr/bin/env python3
"""Build importsnapshot payload for the options-flow-board artifact.
Reads the newest report dir, baselines, premarket, and prior-OHLC files,
normalizes per the cron body, writes snapshot_payload.json."""
import csv, json, os, glob, sys, urllib.request
from datetime import datetime, timezone

SCAN = os.path.expanduser("~/workspace/options-scanner")

# newest dated report dir
dirs = sorted(glob.glob(os.path.join(SCAN, "reports", "*")))
report_dir = dirs[-1]
full_path = os.path.join(report_dir, "options_volume.json")
light_path = os.path.join(report_dir, "options_volume_light.json")

with open(full_path) as f:
    report = json.load(f)
report_path = full_path

# TIERED SCANNING: If a light scan (top 80 tickers) is newer than the full scan,
# merge its fresh rows into the full scan's rows. This gives us 5-min freshness
# for the movers that matter, with full-universe coverage from the last full scan.
# Light rows override full rows for the same ticker (they're newer).
if os.path.exists(light_path):
    light_mtime = os.path.getmtime(light_path)
    full_mtime = os.path.getmtime(full_path)
    if light_mtime > full_mtime:
        try:
            with open(light_path) as f:
                light_report = json.load(f)
            light_rows = {r["ticker"].upper(): r
                          for r in light_report.get("rows", [])
                          if r.get("ticker")}
            if light_rows:
                # Merge: start with full rows, override with light rows
                merged = []
                seen = set()
                for r in light_report.get("rows", []):
                    t = r.get("ticker", "").upper()
                    if t:
                        merged.append(r)
                        seen.add(t)
                for r in report.get("rows", []):
                    t = r.get("ticker", "").upper()
                    if t and t not in seen:
                        merged.append(r)
                report["rows"] = merged
                # sourceAsOf uses the light file's mtime (it's the freshest data)
                report_path = light_path
                print(f"Merged {len(light_rows)} light rows into "
                      f"{len(report['rows'])} total", file=sys.stderr)
        except Exception as ex:
            print(f"WARN: light merge failed ({ex}), using full scan only",
                  file=sys.stderr)

# baselines: symbol -> avg90_volume
baselines = {}
with open(os.path.join(SCAN, "baselines_90d.csv")) as f:
    for row in csv.DictReader(f):
        try:
            baselines[row["symbol"].strip().upper()] = float(row["avg90_volume"])
        except (ValueError, KeyError):
            pass

# prior OHLC for the prior trading day (market date minus weekends/holidays is
# overkill here; body says read prev_ohlc_<market-date>.json when present, and
# the existing file is the prior session's)
market_date = report.get("date")
prev_candidates = sorted(glob.glob(os.path.join(SCAN, f"prev_ohlc_*.json")))
prev_ohlc = {}
if prev_candidates:
    with open(prev_candidates[-1]) as f:
        prev_ohlc = json.load(f)

# premarket gaps
premarket_rows = []
pm_path = os.path.join(SCAN, "premarket.json")
if os.path.exists(pm_path):
    with open(pm_path) as f:
        pm = json.load(f)
    for g in pm.get("gaps", []):
        t = str(g.get("ticker", "")).upper()
        try:
            gap = float(g.get("gap")); price = float(g.get("pm_price"))
            rel = float(g.get("rel")); pc = float(g.get("pc"))
        except (TypeError, ValueError):
            continue
        pos = str(g.get("pos", "")).lower()
        if pos not in ("above", "below", "inside"):
            pos = "above" if gap >= 0 else "below"
        try:
            prior_chg = float(g.get("ychg", 0)); prior_high = float(g.get("ph"))
            prior_low = float(g.get("pl"))
        except (TypeError, ValueError):
            prior_chg, prior_high, prior_low = 0.0, None, None
        premarket_rows.append({
            "ticker": t, "gap": round(gap, 2), "price": round(price, 2),
            "quoteAsOf": g.get("pm_time"),
            "marketCapB": g.get("marketCapB"),
            "relativeVolume": round(rel, 2), "pcRatio": round(pc, 2),
            "lean": str(g.get("lean", "")), "priorDay": str(g.get("yday", "")),
            "priorChange": round(prior_chg, 2),
            "priorHigh": None if prior_high is None else round(prior_high, 2),
            "priorLow": None if prior_low is None else round(prior_low, 2),
            "position": pos,
        })

def num(v):
    try:
        x = float(v)
        return x
    except (TypeError, ValueError):
        return None

def rnd(v, n=4):
    return None if v is None else round(v, n)

# Market-wide top movers (Robinhood-style filter): Yahoo predefined
# day_gainers / day_losers screeners, 50 rows each. Independent of the
# options universe — this is what powers the full-market movers section.
def fetch_screener(scr_id, count=50):
    url = ("https://query1.finance.yahoo.com/v1/finance/screener/"
           f"predefined/saved?scrIds={scr_id}&count={count}")
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.load(resp)
    except Exception as e:
        print(f"WARN: screener {scr_id} fetch failed: {e}", file=sys.stderr)
        return []
    quotes = (data.get("finance", {}).get("result") or [{}])[0].get("quotes") or []
    out = []
    for q in quotes:
        t = str(q.get("symbol") or "").upper()
        price = num(q.get("regularMarketPrice"))
        chg = num(q.get("regularMarketChangePercent"))
        if not t or price is None or chg is None:
            continue
        out.append({"t": t, "price": round(price, 2), "chgPct": round(chg, 2)})
    return out

movers = []
for r in report.get("rows", []):
    ticker = str(r.get("ticker", "")).upper()
    base = baselines.get(ticker)
    chg = num(r.get("chg_pct"))
    vol = num(r.get("options_volume"))
    if not ticker or base is None or base <= 0 or chg is None or vol is None:
        continue
    if abs(chg) < 2:
        continue
    price = num(r.get("price"))
    if price is None:
        continue
    prev = prev_ohlc.get(ticker) or prev_ohlc.get(ticker.upper()) or {}
    prev_high = rnd(num(prev.get("prev_high")))
    prev_low = rnd(num(prev.get("prev_low")))
    if prev_high is None or prev_low is None:
        breakout = "unknown"
    elif price > prev_high:
        breakout = "above"
    elif price < prev_low:
        breakout = "below"
    else:
        breakout = "inside"
    rel = vol / base
    movers.append({
        "ticker": ticker,
        "name": str(r.get("name", "")),
        "price": rnd(price),
        "changePct": rnd(chg),
        "optionsVolume": int(vol),
        "callVolume": int(num(r.get("call_volume")) or 0),
        "putVolume": int(num(r.get("put_volume")) or 0),
        "pcRatio": rnd(num(r.get("pc_ratio")) or 0),
        "relativeVolume": round(rel, 2),
        "dayHigh": rnd(num(r.get("day_high"))),
        "dayLow": rnd(num(r.get("day_low"))),
        "prevHigh": prev_high,
        "prevLow": prev_low,
        "side": "bull" if chg > 0 else "bear",
        "ignited": rel >= 3,
        "breakout": breakout,
    })

covered = len(movers)
bullish = sorted([m for m in movers if m["side"] == "bull"],
                 key=lambda m: m["changePct"], reverse=True)[:25]
bearish = sorted([m for m in movers if m["side"] == "bear"],
                 key=lambda m: m["changePct"])[:25]

source_as_of = datetime.fromtimestamp(
    os.path.getmtime(report_path), tz=timezone.utc).isoformat().replace("+00:00", "Z")

# nextScheduledScanAt: the intraday-options-momentum scheduler's exact
# next_run_at_utc for this run, passed via NEXT_RUN_AT_UTC env var (or argv[1]).
# Verified by the runner against the weekday / 08:30-15:05 America/Chicago /
# NYSE-trading-day gate before this script is invoked; null when absent.
def next_scan_at():
    raw = os.environ.get("NEXT_RUN_AT_UTC", "")
    if not raw and len(sys.argv) > 1:
        raw = sys.argv[1]
    try:
        ts = int(raw)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")

next_scan = next_scan_at()

market_movers = {
    "asOf": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "gainers": fetch_screener("day_gainers")[:50],  # action schema maxItems 50
    "losers": fetch_screener("day_losers")[:50],  # action schema maxItems 50
}
print(f"marketMovers: gainers={len(market_movers['gainers'])} "
      f"losers={len(market_movers['losers'])}", file=sys.stderr)

payload = {
    "marketDate": market_date,
    "sourceAsOf": source_as_of,
    "baselineLabel": "90-day average options volume",
    "scanned": report.get("scanned", 0),
    "covered": covered,
    "bullish": bullish,
    "bearish": bearish,
    "premarket": premarket_rows,
    "marketRegime": [],
    "marketMovers": market_movers,
    "nextScheduledScanAt": next_scan,
}

out = os.path.join(SCAN, "snapshot_payload.json")
with open(out, "w") as f:
    json.dump(payload, f)
print(f"wrote {out}: covered={covered} bull={len(bullish)} bear={len(bearish)} "
      f"premarket={len(premarket_rows)} scanned={payload['scanned']} "
      f"nextScheduledScanAt={next_scan}")
