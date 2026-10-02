#!/usr/bin/env python3
"""Shared level-derivation for the morning breakout watchlist.

Reuse-first: the four-level rules and premarket-bar logic mirror
~/workspace/options-scanner/pkg_src/server.py (yahoo_chart / compute_signals),
which were verified live against Yahoo's own 15m candles.

Level rules (mechanical, no opinions):
  PDH/PDL  previous trading day's high/low, from daily bars (last completed
           daily bar strictly before the session date in ET).
  PMH/PML  premarket session (04:00-09:30 ET) high/low, from this session's
           own 1-minute premarket bars (includePrePost=true).
  First 15m candle = 09:30-09:45 ET regular-session 1m bars, bucketed to the
           09:30 anchor, exactly like the board's to_15m().
  Break flag = first-15m candle close through BOTH highs (bullish: above PDH
           and PMH) or BOTH lows (bearish: below PDL and PML). A true breakout
           must clear all overhead resistance, not just one level — matches
           the user's Pine indicator BREAK rule as clarified 2026-10-01.
"""
import datetime
import json
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}
YHOSTS = ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]

# Full-session NYSE holidays through 2027 (reused from the intraday board gate).
NYSE_HOLIDAYS = {
    "2026-11-26", "2026-12-25",
    "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26",
    "2027-05-31", "2027-06-18", "2027-07-05", "2027-09-06",
    "2027-11-25", "2027-12-24",
}

SHORTLIST_N = 22  # watchlist shortlist size


def et_now():
    return datetime.datetime.now(ET)


def is_trading_morning(now=None):
    """True only on a NYSE trading morning (Mon-Fri, not a listed holiday)."""
    now = now or et_now()
    if now.weekday() >= 5:
        return False, "weekend"
    if now.strftime("%Y-%m-%d") in NYSE_HOLIDAYS:
        return False, "nyse-holiday"
    return True, "ok"


def yahoo_json(path):
    """Fetch a v8 chart path; host rotation + one 429 backoff. Raises on failure."""
    last = None
    for host in YHOSTS:
        try:
            req = urllib.request.Request(f"https://{host}{path}", headers=UA)
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.load(r)["chart"]["result"][0]
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 429:
                time.sleep(30)
            continue
        except Exception as e:  # noqa: BLE001 - network flakiness, try next host
            last = e
            continue
    raise RuntimeError(f"yahoo fetch failed for {path}: {last}")


def fetch_intraday_bars(sym):
    """1m bars incl. pre/post for the current session. Returns (bars, last_px, meta)
    with bars as (epoch,o,h,l,c) 5-tuples."""
    res = yahoo_json(f"/v8/finance/chart/{sym}?interval=1m&range=1d&includePrePost=true")
    ts = res.get("timestamp") or []
    q = (res.get("indicators", {}).get("quote") or [{}])[0]
    closes = q.get("close") or []
    opens = q.get("open") or []
    highs = q.get("high") or []
    lows = q.get("low") or []
    bars = []
    for t, o, h, l, c in zip(ts, opens, highs, lows, closes):
        if not c:
            continue
        bars.append((t, o or c, h or c, l or c, c))
    meta = res.get("meta") or {}
    price = meta.get("regularMarketPrice") or (bars[-1][4] if bars else None)
    return bars, price, meta


def fetch_daily_bars(sym):
    """Daily bars (regular session). Returns list of (date_str, high, low, close)."""
    res = yahoo_json(f"/v8/finance/chart/{sym}?interval=1d&range=5d")
    ts = res.get("timestamp") or []
    q = (res.get("indicators", {}).get("quote") or [{}])[0]
    out = []
    for t, h, l, c in zip(ts, q.get("high") or [], q.get("low") or [], q.get("close") or []):
        if not c:
            continue
        d = datetime.datetime.fromtimestamp(t, ET).strftime("%Y-%m-%d")
        out.append((d, h or c, l or c, c))
    return out


def et_minutes(epoch):
    d = datetime.datetime.fromtimestamp(epoch, ET)
    return d.hour * 60 + d.minute, d.strftime("%Y-%m-%d")


def compute_ticker_levels(sym, session_date=None, intraday=None, daily=None):
    """Return level dict for one ticker. Missing levels are None, never invented.

    session_date: ET date string of the trading session (default: today ET).
    intraday/daily may be pre-fetched to avoid double requests.
    """
    if session_date is None:
        session_date = et_now().strftime("%Y-%m-%d")
    rec = {"ticker": sym, "pdh": None, "pdl": None, "pmh": None, "pml": None,
           "ref_px": None, "prior_close": None, "session_date": session_date,
           "prev_day": None, "note": ""}
    # --- PDH/PDL: last completed daily bar strictly before the session date ---
    try:
        daily = daily if daily is not None else fetch_daily_bars(sym)
        prior = [b for b in daily if b[0] < session_date]
        if prior:
            d, h, l, c = prior[-1]
            rec["pdh"], rec["pdl"], rec["prior_close"], rec["prev_day"] = (
                round(h, 2), round(l, 2), round(c, 2), d)
        else:
            rec["note"] = "no completed daily bar before session"
    except Exception as e:  # noqa: BLE001
        rec["note"] = f"daily fetch failed: {type(e).__name__}"
    # --- PMH/PML: this session's own premarket 1m bars ---
    try:
        bars, price, _meta = intraday if intraday is not None else fetch_intraday_bars(sym)
        ph = pl = None
        last_pm_px = None
        for t, _o, h, l, _c in bars:
            mins, day = et_minutes(t)
            if day != session_date:
                continue
            if mins < 570:  # 04:00-09:30 ET premarket
                ph = h if ph is None or h > ph else ph
                pl = l if pl is None or l < pl else pl
                last_pm_px = _c
        rec["pmh"] = round(ph, 2) if ph else None
        rec["pml"] = round(pl, 2) if pl else None
        rec["ref_px"] = round(last_pm_px, 2) if last_pm_px else rec["prior_close"]
        if ph is None:
            extra = "no premarket bars yet" if not bars else "no premarket trades"
            rec["note"] = (rec["note"] + "; " if rec["note"] else "") + extra
    except Exception as e:  # noqa: BLE001
        extra = f"intraday fetch failed: {type(e).__name__}"
        rec["note"] = (rec["note"] + "; " if rec["note"] else "") + extra
    return rec


def nearest_level(rec):
    """(distance_pct, level_name, level_px) of the closest available level to ref_px."""
    if not rec["ref_px"]:
        return None, None, None
    best = (None, None, None)
    for name, key in (("PDH", "pdh"), ("PDL", "pdl"), ("PMH", "pmh"), ("PML", "pml")):
        px = rec[key]
        if not px:
            continue
        d = abs(rec["ref_px"] - px) / rec["ref_px"] * 100
        if best[0] is None or d < best[0]:
            best = (d, name, px)
    return best


def rank_universe(tickers, session_date=None, workers=8):
    """Compute levels for every ticker (chunked, throttled) and rank by
    distance to the nearest level. Returns (ranked, missing) where ranked is
    a list of recs sorted nearest-first and missing are recs with no levels."""
    session_date = session_date or et_now().strftime("%Y-%m-%d")

    def one(sym):
        return compute_ticker_levels(sym, session_date=session_date)

    recs = []
    for i in range(0, len(tickers), 50):
        chunk = tickers[i:i + 50]
        with ThreadPoolExecutor(max_workers=workers) as ex:
            recs.extend(ex.map(one, chunk))

    ranked, missing = [], []
    for r in recs:
        d, _n, _p = nearest_level(r)
        if d is None:
            missing.append(r)
        else:
            r["near_dist"], r["near_name"], r["near_px"] = d, _n, _p
            ranked.append(r)
    ranked.sort(key=lambda r: r["near_dist"])
    return ranked, missing


def first_15m_close(bars, session_date):
    """Close of the 09:30-09:45 ET candle from 1m bars; None if no bars at all.

    Bucketing mirrors pkg_src/server.py to_15m(): 15m buckets anchored at 570.
    The close is the last 1m print inside the window, whatever the bar count:
    thin names trade sparsely and that last print is still the candle's close.
    (A window with zero bars means the feed has no data — stay silent.)
    """
    closes = []
    for t, o, h, l, c in bars:
        mins, day = et_minutes(t)
        if day != session_date or mins < 570 or mins >= 585:
            continue
        closes.append(c)
    if not closes:
        return None, 0
    return closes[-1], len(closes)


def evaluate_break(rec, close_px):
    """Mechanical break check: first-15m close through BOTH highs (bullish)
    or BOTH lows (bearish). A true breakout must clear all overhead
    resistance (PDH and PMH), not just one level; same for breakdowns.
    Returns (side, broken_names) or (None, [])."""
    if close_px is None:
        return None, []
    # Bullish: close above BOTH PDH and PMH (all available highs)
    highs = [(n, rec[k]) for n, k in (("PDH", "pdh"), ("PMH", "pmh")) if rec.get(k)]
    lows = [(n, rec[k]) for n, k in (("PDL", "pdl"), ("PML", "pml")) if rec.get(k)]
    if highs and all(close_px > px for _, px in highs):
        return "bull", [n for n, _ in highs]
    if lows and all(close_px < px for _, px in lows):
        return "bear", [n for n, _ in lows]
    return None, []


def fmt(x):
    if x is None:
        return "—"
    s = f"{x:.2f}".rstrip("0").rstrip(".")
    return s
