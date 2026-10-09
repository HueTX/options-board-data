#!/usr/bin/env python3
"""15-minute confirmed closes poller — runs every 5 min during market hours.

For each of the 311 optionable tickers:
  - Latest COMPLETED 15m candle close (not the forming one)
  - PDH/PDL: prior trading day's high/low (daily bars)
  - PMH/PML: today's premarket high/low (04:00-09:30 ET, 1m bars)

Classification (Hubert's rule, corrected 2026-10-01):
  - BULLISH: 15m close > PDH OR close > PMH (close above ANY overhead level)
  - BEARISH: 15m close < PDL AND close < PML (breaks all support)

Signal discipline (2026-10-09): only the FIRST 15m candle closing through a
level is listed. A ticker that broke on an earlier candle is stale — the
follow-up move is the trade, and it starts on candle one. Later candles
through the same level are not re-listed ("continuing" is gone); per-ticker
sides persist across polls so nothing is misreported as new.

State tracking across polls: each ticker is new / flipped / dropped.
Output: fifteen_min_closes.json with asOf timestamp, consumed by the
artifact's 15m-closes view and the standalone's 15m-closes.html page.
"""
import datetime
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)
# levels_lib.py lives alongside this script in the repo

import levels_lib as L
from universe import TICKERS

ET = ZoneInfo("America/New_York")
CT = ZoneInfo("America/Chicago")
OUT = os.path.join(BASE, "fifteen_min_closes.json")

NYSE_HOLIDAYS = {
    "2026-11-26", "2026-12-25",
    "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26",
    "2027-05-31", "2027-06-18", "2027-07-05", "2027-09-06",
    "2027-11-25", "2027-12-24",
}


def market_hours_gate():
    now_ct = datetime.datetime.now(CT)
    if now_ct.weekday() >= 5:
        return False, "weekend"
    if now_ct.strftime("%Y-%m-%d") in NYSE_HOLIDAYS:
        return False, "holiday"
    mins = now_ct.hour * 60 + now_ct.minute
    if not (510 <= mins <= 905):  # 08:30 - 15:05 CT
        return False, "outside-hours"
    return True, "ok"


def latest_completed_15m(bars, session_date):
    """Return (close, candle_label) of the latest COMPLETED 15m candle.

    15m buckets anchored at 09:30 ET: [570,585), [585,600), ...
    A bucket is complete only when its end time has passed.
    """
    now_et = datetime.datetime.now(ET)
    now_mins = now_et.hour * 60 + now_et.minute
    today = now_et.strftime("%Y-%m-%d")

    # Group 1m bars into 15m buckets
    buckets = {}
    for t, o, h, l, c in bars:
        mins, day = L.et_minutes(t)
        if day != session_date or mins < 570:
            continue
        bucket_start = 570 + ((mins - 570) // 15) * 15
        buckets.setdefault(bucket_start, []).append((t, c))

    # Find latest bucket whose end time has passed
    best = None
    for start, closes in buckets.items():
        end = start + 15
        # Bucket is complete if we're past its end (with 1-min grace for data latency)
        # or if it's not today's session (historical)
        if session_date != today or now_mins >= end + 1:
            closes.sort()  # sort by timestamp
            close_px = closes[-1][1]
            if best is None or start > best[0]:
                best = (start, close_px)

    if not best:
        return None, None
    start, close_px = best
    h, m = divmod(start, 60)
    label = f"{h:02d}:{m:02d} ET"
    return close_px, label


def poll_one(sym, session_date):
    """Poll one ticker. Returns dict or None on failure."""
    try:
        bars, _px, _meta = L.fetch_intraday_bars(sym)
    except Exception:
        return None
    try:
        daily = L.fetch_daily_bars(sym)
    except Exception:
        daily = []

    # PDH/PDL: last completed daily bar before session
    pdh = pdl = None
    prior = [b for b in daily if b[0] < session_date]
    if prior:
        _, h, l, _c = prior[-1]
        pdh, pdl = round(h, 2), round(l, 2)

    # PMH/PML: today's premarket 1m bars
    pmh = pml = None
    for t, _o, h, l, _c in bars:
        mins, day = L.et_minutes(t)
        if day != session_date or mins >= 570:
            continue
        pmh = h if pmh is None or h > pmh else pmh
        pml = l if pml is None or l < pml else pml
    if pmh:
        pmh, pml = round(pmh, 2), round(pml, 2)

    close_px, candle = latest_completed_15m(bars, session_date)
    if close_px is None:
        return None
    close_px = round(close_px, 2)

    # Hubert's rule (corrected 2026-10-01): bullish = close above ANY overhead level
    side = None
    if (pdh and close_px > pdh) or (pmh and close_px > pmh):
        side = "bullish"
    elif pdl and pml and close_px < pdl and close_px < pml:
        side = "bearish"

    # --- Chart data: 15m regular-session candles + 5m premarket candles ---
    candles15m, premarket5m = [], []
    b15, b5 = {}, {}
    for t, o, h, l, c in bars:
        mins, day = L.et_minutes(t)
        if day != session_date:
            continue
        if mins >= 570:
            bkt = 570 + ((mins - 570) // 15) * 15
            b = b15.setdefault(bkt, {"t": bkt, "o": o, "h": h, "l": l, "c": c})
        elif mins >= 240:
            bkt = 240 + ((mins - 240) // 5) * 5
            b = b5.setdefault(bkt, {"t": bkt, "o": o, "h": h, "l": l, "c": c})
        else:
            continue
        b["h"] = max(b["h"], h)
        b["l"] = min(b["l"], l)
        b["c"] = c  # 1m bars arrive in order; last close wins
    # Hubert's rule: never include or evaluate a forming candle. Drop the
    # in-progress 15m bucket from chart data (signals already use only
    # completed candles via latest_completed_15m).
    _now_et = datetime.datetime.now(ET)
    if _now_et.strftime("%Y-%m-%d") == session_date:
        _now_mins = _now_et.hour * 60 + _now_et.minute
        if _now_mins >= 570:
            _forming = 570 + ((_now_mins - 570) // 15) * 15
            b15 = {k: v for k, v in b15.items() if k < _forming}
    def pack(bkts):
        out = []
        for k in sorted(bkts):
            b = bkts[k]
            hh, mm = divmod(k, 60)
            out.append({"t": f"{hh:02d}:{mm:02d}", "o": round(b["o"], 2),
                        "h": round(b["h"], 2), "l": round(b["l"], 2),
                        "c": round(b["c"], 2)})
        return out
    chart = {
        "levels": {k: v for k, v in {"pdh": pdh, "pdl": pdl, "pmh": pmh, "pml": pml}.items() if v is not None},
        "candles15m": pack(b15),
        "premarket": pack(b5),
    }

    return {
        "ticker": sym, "close": close_px, "candle": candle,
        "pdh": pdh, "pdl": pdl, "pmh": pmh, "pml": pml,
        "side": side, "chart": chart,
    }


def main():
    ok, why = market_hours_gate()
    now_et = datetime.datetime.now(ET)
    session_date = now_et.strftime("%Y-%m-%d")

    if not ok:
        # Outside hours: write a market-closed marker so the page shows honest state
        out = {
            "asOf": now_et.isoformat(),
            "marketDate": session_date,
            "marketOpen": False,
            "reason": why,
            "bullish": [], "bearish": [],
            "newBullish": [], "newBearish": [],
            "dropped": [], "flipped": [],
            "scanned": 0,
        }
        json.dump(out, open(OUT, "w"))
        print(json.dumps({"ok": True, "marketOpen": False, "reason": why}))
        return 0

    # Load previous per-ticker sides for new/dropped/flipped tracking.
    # Persisted as a full side map (not list membership): a ticker whose
    # break is old news is excluded from the output lists but must still be
    # recognized next poll, otherwise it would be re-reported as "new" on
    # every subsequent candle. Only the FIRST 15m candle closing through a
    # level is a signal — later candles through the same level are stale.
    prev_sides = {}
    if os.path.exists(OUT):
        try:
            prev_data = json.load(open(OUT))
            prev_sides = prev_data.get("sides") or {}
            if not prev_sides:
                # Backward compat with files written before the sides map.
                for r in prev_data.get("bullish", []):
                    prev_sides[r["ticker"]] = "bullish"
                for r in prev_data.get("bearish", []):
                    prev_sides[r["ticker"]] = "bearish"
        except Exception:
            prev_sides = {}

    results = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(poll_one, sym, session_date): sym for sym in TICKERS}
        for fut in futures:
            try:
                r = fut.result(timeout=30)
                if r:
                    results.append(r)
            except Exception:
                pass

    bullish, bearish = [], []
    new_bullish, new_bearish, dropped, flipped = [], [], [], []
    curr_sides = {}

    for r in results:
        curr_sides[r["ticker"]] = r["side"]
        prev_side = prev_sides.get(r["ticker"])
        if r["side"] == "bullish":
            # The board's import15mcloses schema requires pmh as a number. When a
            # ticker has no premarket bars (e.g. thin ETFs), pmh is undefined and
            # the import is rejected — skip such tickers rather than fabricating
            # or mutating the payload for a single run.
            if r.get("pdh") is None or r.get("pmh") is None:
                continue
            if prev_side == "bullish":
                continue  # break on an earlier candle: stale, not a fresh signal
            entry = {"ticker": r["ticker"], "close": r["close"], "candle": r["candle"],
                     "pdh": r["pdh"], "pmh": r["pmh"]}
            entry["status"] = "new" if prev_side is None else "flipped"
            (new_bullish if prev_side is None else flipped).append(r["ticker"])
            bullish.append(entry)
        elif r["side"] == "bearish":
            if r.get("pdl") is None or r.get("pml") is None:
                continue
            if prev_side == "bearish":
                continue  # break on an earlier candle: stale, not a fresh signal
            entry = {"ticker": r["ticker"], "close": r["close"], "candle": r["candle"],
                     "pdl": r["pdl"], "pml": r["pml"]}
            entry["status"] = "new" if prev_side is None else "flipped"
            (new_bearish if prev_side is None else flipped).append(r["ticker"])
            bearish.append(entry)

    # Dropped: had a side last poll, scanned now, on neither side
    for sym, side in prev_sides.items():
        if side in ("bullish", "bearish") and sym in curr_sides and curr_sides[sym] is None:
            dropped.append({"ticker": sym, "was": side})

    bullish.sort(key=lambda r: r["ticker"])
    bearish.sort(key=lambda r: r["ticker"])

    # Most common candle label (computed before the chart bundle needs it)
    candles = [r.get("candle") for r in results if r.get("candle")]
    candle_label = max(set(candles), key=candles.count) if candles else None

    # Chart bundle: only for tickers currently on the lists (keeps file lean)
    chart_by_ticker = {r["ticker"]: r.get("chart") for r in results if r.get("chart")}
    charts = {}
    for entry in bullish:
        c = chart_by_ticker.get(entry["ticker"])
        if c:
            charts[entry["ticker"]] = c
    for entry in bearish:
        c = chart_by_ticker.get(entry["ticker"])
        if c:
            charts[entry["ticker"]] = c
    charts_out = {
        "asOf": now_et.isoformat(),
        "marketDate": session_date,
        "marketOpen": True,
        "candle": candle_label,
        "charts": charts,
    }
    json.dump(charts_out, open(os.path.join(BASE, "fifteen_min_charts.json"), "w"))

    out = {
        "asOf": now_et.isoformat(),
        "marketDate": session_date,
        "marketOpen": True,
        "candle": candle_label,
        "bullish": bullish,
        "bearish": bearish,
        "newBullish": sorted(new_bullish),
        "newBearish": sorted(new_bearish),
        "dropped": sorted(dropped, key=lambda d: d["ticker"]),
        "flipped": sorted(flipped),
        "scanned": len(results),
        "universe": len(TICKERS),
        # Full per-ticker side map for next poll's new/dropped/flipped
        # tracking (see prev_sides above). Not for display.
        "sides": curr_sides,
    }
    json.dump(out, open(OUT, "w"))
    print(json.dumps({
        "ok": True, "marketOpen": True,
        "bullish": len(bullish), "bearish": len(bearish),
        "scanned": len(results), "candle": candle_label,
        "asOf": out["asOf"],
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())
