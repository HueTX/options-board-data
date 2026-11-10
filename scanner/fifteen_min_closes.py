#!/usr/bin/env python3
"""15-minute confirmed closes poller — spec build, 2026-11-10.

Polls the 311-ticker universe every 5 minutes during market hours and
answers one question per ticker: did the most recent FINISHED 15-minute
candle close above its overhead levels (PDH / PMH) or below its support
levels (PDL / PML)? A candle that is still forming is never evaluated,
displayed, or signaled on.

Data source: Yahoo Finance v8 chart API only (no key, no paid data) —
two calls per ticker per poll, via levels_lib (browser User-Agent,
query1/query2 host rotation, 15s timeout, on HTTP 429 sleep 30s and
retry the other host, then give up on that ticker for this poll):
  1. intraday 1m, range=1d, includePrePost=true — premarket levels AND
     every 1-minute bar the 15m candles are built from.
  2. daily 1d, range=5d — PDH/PDL/prior close from the last daily bar
     dated BEFORE today's session date.
Per-ticker work runs in a thread pool of 12 workers with a 30-second
cap per ticker; a failed ticker is skipped, never fatal. Nothing is
cached across polls except the previous output file (status tracking).

Levels (per ticker, per poll):
  PDH/PDL — prior trading day's high/low (daily call).
  PMH/PML — max high / min low across today's 1m bars with ET time
            < 09:30. Null when the ticker had no premarket trades;
            never fabricated.

15-minute candles: buckets anchored at 09:30 ET
(bucket start = 570 + floor((et_minutes - 570) / 15) * 15), aggregated
to OHLC from the 1m bars. A bucket is COMPLETED only when the current
ET time is past bucket_end + 1 minute (1-minute grace for data
latency). Only the latest completed bucket is evaluated; the forming
bucket is excluded from all output, including chart series. The
session mean is the mean of the completed 15m closes.

Classification (exact):
  BULLISH — 15m close > PDH OR close > PMH (above ANY overhead level).
  BEARISH — 15m close < PDL AND close < PML (below BOTH supports).
  A bullish entry is emitted only when PDH and PMH are both numeric;
  a bearish entry only when PDL and PML are both numeric. Each entry
  records which levels it cleared (above/below: "both"|"pdh"|"pmh"
  mirrored) and thruPct — how far beyond the FARTHEST cleared level
  the close traveled, in %.

Status tracking across polls (previous output file is the only state;
same-day only — a new session starts clean):
  "new"        — on a list now, was on neither list last poll.
  "flipped"    — was on the opposite list last poll.
  "fading"     — qualified on an earlier candle today, but the latest
                 completed close has slipped back inside a level it
                 originally broke: either it no longer qualifies at
                 all (kept on its original side's list for one poll,
                 then dropped if it still does not qualify), or a
                 bullish close that cleared BOTH levels now clears
                 only one. The entry stays on the list, badged FADING.
  "continuing" — on the same list last poll, still qualifies.
  "dropped"    — was on a list last poll, is on neither now, and WAS
                 successfully scanned this poll. Reported in the
                 output, never rendered as a row.
  One candle close through the levels is the signal — a ticker still
  qualifying on later candles is "continuing", never a fresh "new".

Market gate: Mon-Fri, excluding full-session NYSE holidays, 08:30-
15:05 America/Chicago (all candle math in America/New_York). Outside
the gate the output is marketOpen:false with a reason and empty lists
— but a completed same-day file is NEVER overwritten by an older or
empty one, and a poll that scans zero tickers never erases the last
good file. Writes are atomic (tmp + replace).

Outputs (static JSON the page fetches):
  fifteen_min_closes.json — ranked lists + status arrays + counts.
  fifteen_min_charts.json — per-listed-ticker chart data only:
    {levels (numeric only), closes15m: [{t, c}] for every COMPLETED
     bucket today, mean} plus legacy candles15m/premarket series the
     detail chart page consumes.
"""
import datetime
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout
from zoneinfo import ZoneInfo

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

import levels_lib as L
from universe import TICKERS

ET = ZoneInfo("America/New_York")
CT = ZoneInfo("America/Chicago")
OUT = os.path.join(BASE, "fifteen_min_closes.json")
CHARTS_OUT = os.path.join(BASE, "fifteen_min_charts.json")
REPO_ROOT = os.path.dirname(BASE)

NYSE_HOLIDAYS = {
    "2026-11-26", "2026-12-25",
    "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26",
    "2027-05-31", "2027-06-18", "2027-07-05", "2027-09-06",
    "2027-11-25", "2027-12-24",
}

WORKERS = 12
PER_TICKER_TIMEOUT_S = 30


# ------------------------------------------------------------------ tables
def load_tables():
    """Names + market caps joined by ticker. Missing values stay null —
    never invented."""
    names = {}
    for path in (os.path.join(BASE, "names_cache.json"),
                 os.path.join(REPO_ROOT, "names_cache.json")):
        try:
            with open(path) as f:
                names.update(json.load(f))
        except Exception:
            pass
    caps = {}
    try:
        with open(os.path.join(REPO_ROOT, "market_caps.json")) as f:
            caps = json.load(f).get("caps", {})
    except Exception:
        caps = {}
    return names, caps


NAMES, CAPS = load_tables()


# ------------------------------------------------------------------ gate
def market_hours_gate(now_ct=None):
    now_ct = now_ct or datetime.datetime.now(CT)
    if now_ct.weekday() >= 5:
        return False, "weekend"
    if now_ct.strftime("%Y-%m-%d") in NYSE_HOLIDAYS:
        return False, "holiday"
    mins = now_ct.hour * 60 + now_ct.minute
    if not (510 <= mins <= 905):  # 08:30 - 15:05 CT
        return False, "outside-hours"
    return True, "ok"


# ------------------------------------------------------------------ candles
def bucket_label(start_mins):
    h, m = divmod(start_mins, 60)
    return f"{h:02d}:{m:02d} ET"


def build_completed_buckets(bars, session_date, now_et=None):
    """Aggregate today's regular-session 1m bars into 15m buckets
    anchored at 09:30 ET and return ONLY the completed ones, in order:
    [{start, o, h, l, c}]. A bucket is completed when the current ET
    time is past bucket_end + 1 minute; for a past session date every
    bucket is complete. The forming bucket is never returned."""
    now_et = now_et or datetime.datetime.now(ET)
    is_today = session_date == now_et.strftime("%Y-%m-%d")
    now_mins = now_et.hour * 60 + now_et.minute + now_et.second / 60.0
    buckets = {}
    for t, o, h, l, c in sorted(bars):
        mins, day = L.et_minutes(t)
        if day != session_date or mins < 570:
            continue
        start = 570 + ((mins - 570) // 15) * 15
        b = buckets.get(start)
        if b is None:
            buckets[start] = {"start": start, "o": o, "h": h, "l": l, "c": c}
        else:
            b["h"] = max(b["h"], h)
            b["l"] = min(b["l"], l)
            b["c"] = c  # bars are time-ordered; last close wins
    out = []
    for start in sorted(buckets):
        if is_today and now_mins < start + 15 + 1:
            continue  # still forming (or inside the 1-minute grace)
        out.append(buckets[start])
    return out


def premarket_levels(bars, session_date):
    """PMH/PML from today's 1m bars with ET time < 09:30. (None, None)
    when the ticker printed no premarket bars."""
    pmh = pml = None
    for t, _o, h, l, _c in bars:
        mins, day = L.et_minutes(t)
        if day != session_date or mins >= 570:
            continue
        pmh = h if pmh is None or h > pmh else pmh
        pml = l if pml is None or l < pml else pml
    return (round(pmh, 2) if pmh is not None else None,
            round(pml, 2) if pml is not None else None)


def premarket_5m_series(bars, session_date):
    """Legacy 5m premarket candles for the detail chart page."""
    b5 = {}
    for t, o, h, l, c in sorted(bars):
        mins, day = L.et_minutes(t)
        if day != session_date or mins < 240 or mins >= 570:
            continue
        start = 240 + ((mins - 240) // 5) * 5
        b = b5.get(start)
        if b is None:
            b5[start] = {"o": o, "h": h, "l": l, "c": c}
        else:
            b["h"] = max(b["h"], h)
            b["l"] = min(b["l"], l)
            b["c"] = c
    out = []
    for start in sorted(b5):
        b = b5[start]
        hh, mm = divmod(start, 60)
        out.append({"t": f"{hh:02d}:{mm:02d}", "o": round(b["o"], 2),
                    "h": round(b["h"], 2), "l": round(b["l"], 2),
                    "c": round(b["c"], 2)})
    return out


# ------------------------------------------------------------------ classify
def classify_close(close, pdh, pmh, pdl, pml):
    """(side, cleared) for one completed close. side is "bullish",
    "bearish" or None; cleared is the set of level keys it broke.
    Bullish needs PDH and PMH both numeric and close above ANY of
    them; bearish needs PDL and PML both numeric and close below
    BOTH of them."""
    if pdh is not None and pmh is not None:
        cleared = set()
        if close > pdh:
            cleared.add("pdh")
        if close > pmh:
            cleared.add("pmh")
        if cleared:
            return "bullish", cleared
    if pdl is not None and pml is not None:
        cleared = set()
        if close < pdl:
            cleared.add("pdl")
        if close < pml:
            cleared.add("pml")
        if len(cleared) == 2:
            return "bearish", cleared
    return None, set()


def thru_pct(close, side, cleared, pdh, pmh, pdl, pml):
    """% beyond the FARTHEST cleared level (bullish: max of the cleared
    highs; bearish: min of the cleared lows). Signed: negative when a
    fading close has slipped back inside that level."""
    if side == "bullish":
        levels = [v for k, v in (("pdh", pdh), ("pmh", pmh)) if k in cleared]
        if not levels:
            return None
        far = max(levels)
        return round((close - far) / far * 100, 2)
    if side == "bearish":
        levels = [v for k, v in (("pdl", pdl), ("pml", pml)) if k in cleared]
        if not levels:
            return None
        far = min(levels)
        return round((far - close) / far * 100, 2)
    return None


def cleared_label(side, cleared):
    if side == "bullish":
        if cleared == {"pdh", "pmh"}:
            return "both"
        if cleared == {"pdh"}:
            return "pdh"
        if cleared == {"pmh"}:
            return "pmh"
    if side == "bearish":
        if cleared == {"pdl", "pml"}:
            return "both"
        if cleared == {"pdl"}:
            return "pdl"
        if cleared == {"pml"}:
            return "pml"
    return "both" if side else None


# ------------------------------------------------------------------ per ticker
def poll_one(sym, session_date, now_et=None):
    """Fetch + evaluate one ticker. Returns a record dict (possibly
    with no completed candle yet), or None when either Yahoo call
    failed — a failed ticker is skipped, never fatal."""
    try:
        bars, _px, _meta = L.fetch_intraday_bars(sym)
        daily = L.fetch_daily_bars(sym)
    except Exception:
        return None

    pdh = pdl = prior_close = None
    prior = [b for b in daily if b[0] < session_date]
    if prior:
        _d, h, l, c = prior[-1]
        pdh, pdl = round(h, 2), round(l, 2)
        prior_close = round(c, 2)

    pmh, pml = premarket_levels(bars, session_date)
    buckets = build_completed_buckets(bars, session_date, now_et=now_et)

    rec = {
        "ticker": sym, "pdh": pdh, "pdl": pdl, "pmh": pmh, "pml": pml,
        "priorClose": prior_close, "buckets": buckets,
        "premarket5m": premarket_5m_series(bars, session_date),
    }
    if not buckets:
        return rec  # scanned OK, but no completed candle yet

    latest = buckets[-1]
    close = round(latest["c"], 2)
    rec["close"] = close
    rec["candle"] = bucket_label(latest["start"])
    rec["candleOpen"] = round(latest["o"], 2)
    rec["candlePct"] = (round((latest["c"] - latest["o"]) / latest["o"] * 100, 2)
                         if latest["o"] else None)
    rec["dayPct"] = (round((close - prior_close) / prior_close * 100, 2)
                     if prior_close else None)
    rec["mean"] = round(sum(b["c"] for b in buckets) / len(buckets), 2)

    # Classify EVERY completed candle: status tracking needs to know
    # whether the ticker qualified on an earlier candle today, which
    # levels it cleared then, and on which side it most recently did.
    evals = []
    for b in buckets:
        side, cleared = classify_close(round(b["c"], 2), pdh, pmh, pdl, pml)
        evals.append((b["start"], side, cleared))
    rec["evals"] = evals
    rec["sideNow"], rec["clearedNow"] = evals[-1][1], evals[-1][2]
    return rec


# ------------------------------------------------------------------ assembly
def assemble(results, prev, session_date, now_et):
    """Turn per-ticker records into the output payload. `prev` is the
    previous output payload for THIS session date (or {})."""
    prev_side, prev_status = {}, {}
    if prev.get("marketDate") == session_date:
        for r in prev.get("bullish", []):
            prev_side[r["ticker"]] = "bullish"
            prev_status[r["ticker"]] = r.get("status")
        for r in prev.get("bearish", []):
            prev_side[r["ticker"]] = "bearish"
            prev_status[r["ticker"]] = r.get("status")

    scanned = [r for r in results if r is not None]
    bullish, bearish, dropped = [], [], []
    charts = {}
    curr_sides = {}

    for r in scanned:
        sym = r["ticker"]
        curr_sides[sym] = r.get("sideNow")
        if "close" not in r:
            continue  # scanned, but no completed candle yet today
        evals = r["evals"]
        earlier = evals[:-1]
        side_now, cleared_now = r["sideNow"], r["clearedNow"]

        # Did it qualify on an earlier candle today, and how strongly?
        earlier_side, earlier_cleared = None, set()
        best_cleared = {"bullish": set(), "bearish": set()}
        for _start, s, cl in evals:
            if s and len(cl) > len(best_cleared[s]):
                best_cleared[s] = cl
        for _start, s, cl in reversed(earlier):
            if s:
                earlier_side, earlier_cleared = s, cl
                break

        emit_side, emit_cleared, status = None, set(), None
        if side_now:
            emit_side, emit_cleared = side_now, cleared_now
            if prev_side.get(sym) and prev_side[sym] != side_now:
                status = "flipped"
            elif (side_now == "bullish"
                  and len(best_cleared["bullish"]) == 2
                  and len(cleared_now) == 1):
                # Cleared BOTH levels on an earlier candle; the latest
                # close slipped back inside one of them — still above
                # the other, so still listed, but fading.
                status = "fading"
            elif prev_side.get(sym) == side_now:
                status = "continuing"
            else:
                status = "new"
        elif earlier_side:
            # Qualified on an earlier candle today; the latest close
            # slipped back inside the level(s) it broke. Keep it on
            # its original side's list as FADING for one poll; if it
            # still has not re-qualified next poll it drops.
            if prev_status.get(sym) == "fading" and prev_side.get(sym) == earlier_side:
                emit_side = None
            else:
                emit_side, emit_cleared = earlier_side, earlier_cleared
                status = "fading"

        if emit_side:
            base = {
                "ticker": sym,
                "name": NAMES.get(sym),
                "close": r["close"],
                "candle": r["candle"],
                "dayPct": r["dayPct"],
                "marketCapB": CAPS.get(sym),
                "candlePct": r["candlePct"],
                "status": status,
            }
            if emit_side == "bullish":
                base.update({
                    "pdh": r["pdh"], "pmh": r["pmh"],
                    "above": cleared_label("bullish", emit_cleared),
                    "thruPct": thru_pct(r["close"], "bullish", emit_cleared,
                                        r["pdh"], r["pmh"], r["pdl"], r["pml"]),
                })
                bullish.append(base)
            else:
                base.update({
                    "pdl": r["pdl"], "pml": r["pml"],
                    "below": cleared_label("bearish", emit_cleared),
                    "thruPct": thru_pct(r["close"], "bearish", emit_cleared,
                                        r["pdh"], r["pmh"], r["pdl"], r["pml"]),
                })
                bearish.append(base)
            curr_sides[sym] = emit_side

            levels = {k: v for k, v in (("pdh", r["pdh"]), ("pdl", r["pdl"]),
                                        ("pmh", r["pmh"]), ("pml", r["pml"]))
                      if v is not None}
            charts[sym] = {
                "levels": levels,
                "closes15m": [{"t": bucket_label(b["start"])[:5],
                               "c": round(b["c"], 2)} for b in r["buckets"]],
                "mean": r["mean"],
                # Legacy series for the detail chart page (15m-chart.html):
                # full OHLC of the same COMPLETED buckets, nothing forming.
                "candles15m": [{"t": bucket_label(b["start"])[:5],
                                "o": round(b["o"], 2), "h": round(b["h"], 2),
                                "l": round(b["l"], 2), "c": round(b["c"], 2)}
                               for b in r["buckets"]],
                "premarket": r["premarket5m"],
            }
        elif prev_side.get(sym):
            # Was listed last poll, on neither list now, and WAS
            # successfully scanned this poll (it is in `scanned`).
            dropped.append({"ticker": sym, "was": prev_side[sym]})

    bullish.sort(key=lambda e: e["ticker"])
    bearish.sort(key=lambda e: e["ticker"])
    dropped.sort(key=lambda d: d["ticker"])

    labels = [r["candle"] for r in scanned if r.get("candle")]
    candle_label = max(set(labels), key=labels.count) if labels else None

    out = {
        "asOf": now_et.isoformat(),
        "marketDate": session_date,
        "marketOpen": True,
        "candle": candle_label,
        "bullish": bullish,
        "bearish": bearish,
        "newBullish": sorted(e["ticker"] for e in bullish if e["status"] == "new"),
        "newBearish": sorted(e["ticker"] for e in bearish if e["status"] == "new"),
        "flipped": sorted(e["ticker"] for e in bullish + bearish
                          if e["status"] == "flipped"),
        "dropped": dropped,
        "scanned": len(scanned),
        "universe": len(TICKERS),
        # Full per-ticker side map for any legacy consumer; the status
        # machine itself derives everything from the lists above.
        "sides": curr_sides,
    }
    charts_out = {
        "asOf": now_et.isoformat(),
        "marketDate": session_date,
        "marketOpen": True,
        "candle": candle_label,
        "charts": charts,
    }
    return out, charts_out


def dump_atomic(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


def load_prev():
    try:
        with open(OUT) as f:
            return json.load(f)
    except Exception:
        return {}


# ------------------------------------------------------------------ main
def main():
    ok, why = market_hours_gate()
    now_et = datetime.datetime.now(ET)
    session_date = now_et.strftime("%Y-%m-%d")
    prev = load_prev()

    if not ok:
        # Outside the gate: honest market-closed marker — but NEVER
        # downgrade a completed same-day file to the marker (the
        # 2026-10-23 failure class: the day's full closes file was
        # replaced by scanned:0 and every strict consumer broke).
        if prev.get("marketDate") == session_date and (prev.get("scanned") or 0) > 0:
            print(json.dumps({"ok": True, "marketOpen": False, "reason": why,
                              "kept": "completed same-day closes file left untouched"}))
            return 0
        out = {
            "asOf": now_et.isoformat(),
            "marketDate": session_date,
            "marketOpen": False,
            "reason": why,
            "candle": None,
            "bullish": [], "bearish": [],
            "newBullish": [], "newBearish": [],
            "dropped": [], "flipped": [],
            "scanned": 0,
            "universe": len(TICKERS),
        }
        dump_atomic(OUT, out)
        dump_atomic(CHARTS_OUT, {
            "asOf": now_et.isoformat(), "marketDate": session_date,
            "marketOpen": False, "candle": None, "charts": {}})
        print(json.dumps({"ok": True, "marketOpen": False, "reason": why}))
        return 0

    results = []
    ex = ThreadPoolExecutor(max_workers=WORKERS)
    try:
        futures = {ex.submit(poll_one, sym, session_date): sym for sym in TICKERS}
        for fut, sym in futures.items():
            try:
                results.append(fut.result(timeout=PER_TICKER_TIMEOUT_S))
            except FutTimeout:
                results.append(None)  # over the per-ticker cap: skipped
            except Exception:
                results.append(None)
    finally:
        ex.shutdown(wait=False, cancel_futures=True)

    scanned_n = sum(1 for r in results if r is not None)
    if scanned_n == 0 and prev.get("marketDate") == session_date \
            and (prev.get("scanned") or 0) > 0:
        # A total poll failure never erases the last good file.
        print(json.dumps({"ok": False, "marketOpen": True, "scanned": 0,
                          "kept": "previous same-day file left untouched"}))
        return 0

    out, charts_out = assemble(results, prev, session_date, now_et)
    dump_atomic(OUT, out)
    dump_atomic(CHARTS_OUT, charts_out)
    print(json.dumps({
        "ok": True, "marketOpen": True,
        "bullish": len(out["bullish"]), "bearish": len(out["bearish"]),
        "scanned": out["scanned"], "candle": out["candle"],
        "asOf": out["asOf"],
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())
