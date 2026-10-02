#!/usr/bin/env python3
"""Daily options-volume scanner.

For every ticker in universe.py, pulls the nearest N option expirations,
sums call + put volume traded today, and ranks tickers by total options
volume. Writes a dated CSV, JSON and a styled HTML report.

Usage:  python3 scan.py [--top N] [--expiries N] [--workers N]
"""
import argparse
import concurrent.futures
import datetime
import glob
import html
import json
import os
import random
import sys
import threading
import time

import pandas as pd
import requests
import yfinance as yf

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from universe import TICKERS, INDEX_PRODUCTS

BASE = os.path.dirname(os.path.abspath(__file__))
REPORTS = os.path.join(BASE, "reports")
HISTORY_CSV = os.path.join(BASE, "history.csv")

# NYSE holidays (full-day closures). Extend as years roll over.
NYSE_HOLIDAYS = {
    # 2026
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03", "2026-05-25",
    "2026-06-19", "2026-07-03", "2026-09-07", "2026-11-26", "2026-12-25",
    # 2027
    "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26", "2027-05-31",
    "2027-06-18", "2027-07-05", "2027-09-06", "2027-11-25", "2027-12-24",
}


def is_trading_day(d=None):
    d = d or datetime.date.today()
    return d.weekday() < 5 and d.isoformat() not in NYSE_HOLIDAYS


# ---------------------------------------------------------------------------
# Yahoo politeness: one shared session (connection reuse, single cookie/
# crumb handshake), a process-wide token bucket (~10 req/s sustained, small
# burst), and exponential backoff on 429/rate-limit responses. Without this
# the scanner fired ~2,500 requests in uncontrolled bursts and Yahoo
# throttled/blocked the IP.
# ---------------------------------------------------------------------------
_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                     "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"}

_session = None
_session_lock = threading.Lock()


def get_session():
    """Single shared requests session for all yfinance calls."""
    global _session
    with _session_lock:
        if _session is None:
            s = requests.Session()
            s.headers.update(_UA)
            adapter = requests.adapters.HTTPAdapter(pool_connections=10,
                                                    pool_maxsize=20,
                                                    max_retries=0)
            s.mount("https://", adapter)
            s.mount("http://", adapter)
            _session = s
    return _session


_BUCKET_RATE = 10.0      # sustained requests/sec across all threads
_BUCKET_BURST = 20.0     # max burst
_bucket_lock = threading.Lock()
_bucket_tokens = _BUCKET_BURST
_bucket_last = time.monotonic()


def _take_token():
    """Block until the token bucket yields one request token."""
    global _bucket_tokens, _bucket_last
    while True:
        with _bucket_lock:
            now = time.monotonic()
            _bucket_tokens = min(_BUCKET_BURST,
                                 _bucket_tokens + (now - _bucket_last) * _BUCKET_RATE)
            _bucket_last = now
            if _bucket_tokens >= 1.0:
                _bucket_tokens -= 1.0
                return
            wait = (1.0 - _bucket_tokens) / _BUCKET_RATE
        time.sleep(wait + random.uniform(0, 0.05))


def _is_rate_limit(ex):
    s = str(ex).lower()
    return ("429" in s or "too many requests" in s or "rate limit" in s
            or "rate-limit" in s)


# Throttling monitor: counts 429/rate-limit hits per scan run.
# If this exceeds ~10 per scan, Yahoo is throttling us and we need to
# reduce the API budget further (fewer tickers, longer intervals).
_rate_limit_hits = 0
_rate_limit_lock = threading.Lock()


def _call(fn):
    """Run a yfinance call through the rate limiter, backing off on 429s."""
    global _rate_limit_hits
    delay = 2.0
    for attempt in range(4):
        _take_token()
        try:
            return fn()
        except Exception as ex:
            if _is_rate_limit(ex):
                with _rate_limit_lock:
                    _rate_limit_hits += 1
            if _is_rate_limit(ex) and attempt < 3:
                time.sleep(delay + random.uniform(0, 1.0))
                delay *= 2
                continue
            raise


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def fetch_bulk_prices(symbols):
    """Fetch day OHLC + prev close for all symbols in 1-2 Yahoo calls.

    Replaces 311 individual fast_info calls (one per ticker) with a single
    yf.download batch. Returns dict: symbol -> {price, prev, day_high, day_low}.
    Missing symbols get an empty dict (caller falls back to fast_info).
    """
    out = {}
    try:
        # period="2d" gives us today + yesterday for prev-close calc
        data = _call(lambda: yf.download(
            " ".join(symbols), period="2d", interval="1d",
            progress=False, auto_adjust=False, session=get_session()))
        if data is None or data.empty:
            return out
        # yfinance returns MultiIndex columns (field, ticker) for multi-ticker
        for sym in symbols:
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    closes = data["Close"][sym].dropna()
                    highs = data["High"][sym].dropna()
                    lows = data["Low"][sym].dropna()
                else:
                    # single ticker fallback (shouldn't happen with 311)
                    closes = data["Close"].dropna()
                    highs = data["High"].dropna()
                    lows = data["Low"].dropna()
                if len(closes) == 0:
                    continue
                price = float(closes.iloc[-1])
                prev = float(closes.iloc[-2]) if len(closes) > 1 else 0.0
                out[sym] = {
                    "price": price,
                    "prev": prev,
                    "day_high": float(highs.iloc[-1]) if len(highs) else 0.0,
                    "day_low": float(lows.iloc[-1]) if len(lows) else 0.0,
                }
            except Exception:
                continue
    except Exception as ex:
        print(f"WARN: bulk price fetch failed ({ex}); falling back to per-ticker",
              file=sys.stderr)
    return out


def scan_one(sym, n_expiries, price_cache=None):
    """Return dict with today's options volume for one ticker, or None."""
    try:
        t = yf.Ticker(sym, session=get_session())
        exps = _call(lambda: t.options)
        if not exps:
            return None
        call_vol = 0
        put_vol = 0
        used = 0
        for e in exps[:n_expiries]:
            try:
                ch = _call(lambda e=e: t.option_chain(e))
                call_vol += int(ch.calls["volume"].fillna(0).sum())
                put_vol += int(ch.puts["volume"].fillna(0).sum())
                used += 1
            except Exception:
                continue
        total = call_vol + put_vol
        if total <= 0:
            return None
        # Prefer bulk-fetched prices (1 batch call for all tickers);
        # fall back to fast_info only if the symbol missed the batch.
        pd_ = (price_cache or {}).get(sym)
        if pd_ and pd_.get("price"):
            price = pd_["price"]
            prev = pd_.get("prev", 0.0)
            day_high = pd_.get("day_high", 0.0)
            day_low = pd_.get("day_low", 0.0)
        else:
            fi = _call(lambda: t.fast_info)
            price = _num(fi.get("lastPrice")) or _num(fi.get("last_price"))
            prev = _num(fi.get("previousClose")) or _num(fi.get("previous_close"))
            day_high = _num(fi.get("dayHigh")) or _num(fi.get("day_high"))
            day_low = _num(fi.get("dayLow")) or _num(fi.get("day_low"))
        chg = (price / prev - 1) * 100 if price and prev else 0.0
        return {
            "ticker": sym,
            "price": round(price, 2),
            "chg_pct": round(chg, 2),
            "options_volume": total,
            "call_volume": call_vol,
            "put_volume": put_vol,
            "pc_ratio": round(put_vol / call_vol, 2) if call_vol else 0.0,
            "day_high": round(day_high, 2),
            "day_low": round(day_low, 2),
            "expiries_scanned": used,
        }
    except Exception:
        return None


def fetch_names(rows, limit=60):
    """Attach company short names for the top rows (one extra call each).

    .info is Yahoo's heaviest, most rate-limited endpoint, so names are
    cached on disk and only uncached tickers are fetched, one per second.
    """
    cache_path = os.path.join(BASE, "names_cache.json")
    try:
        with open(cache_path) as fh:
            cache = json.load(fh)
    except Exception:
        cache = {}
    sess = get_session()
    dirty = False
    for r in rows[:limit]:
        t = r["ticker"]
        if t in cache:
            r["name"] = cache[t]
            continue
        try:
            info = _call(lambda t=t: yf.Ticker(t, session=sess).info)
            name = info.get("shortName") or info.get("longName") or t
        except Exception:
            name = t
        cache[t] = name
        r["name"] = name
        dirty = True
        time.sleep(1.0)
    for r in rows[limit:]:
        r.setdefault("name", cache.get(r["ticker"], r["ticker"]))
    if dirty:
        try:
            with open(cache_path, "w") as fh:
                json.dump(cache, fh)
        except Exception:
            pass
    return rows


def html_report(rows, stamp, scanned, failed, n_expiries):
    rows = [r for r in rows if r["ticker"] not in INDEX_PRODUCTS]

    def fmt(n):
        return f"{n:,}"

    trs = []
    for i, r in enumerate(rows[:60], 1):
        chg = r["chg_pct"]
        cls = "up" if chg >= 0 else "dn"
        arrow = "▲" if chg >= 0 else "▼"
        trs.append(
            "<tr><td class='rk'>{}</td><td class='tk'>{}</td>"
            "<td class='nm'>{}</td><td>${:,.2f}</td>"
            "<td class='{}'>{} {:+.2f}%</td><td class='vol'>{}</td>"
            "<td>{}</td><td>{}</td><td>{:.2f}</td></tr>".format(
                i, html.escape(r["ticker"]), html.escape(r.get("name", r["ticker"])),
                r["price"], cls, arrow, chg, fmt(r["options_volume"]),
                fmt(r["call_volume"]), fmt(r["put_volume"]), r["pc_ratio"]))
    body = "\n".join(trs)
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Options Volume Scanner — {stamp}</title>
<style>
body{{font-family:-apple-system,Helvetica,Arial,sans-serif;background:#0e1116;color:#e8eaed;margin:0;padding:20px}}
h1{{font-size:22px;margin:0 0 4px}} .sub{{color:#9aa0a6;font-size:13px;margin-bottom:16px}}
table{{border-collapse:collapse;width:100%;font-size:14px}}
th{{text-align:left;color:#9aa0a6;font-weight:600;font-size:12px;text-transform:uppercase;
padding:10px 8px;border-bottom:1px solid #2a2e35;position:sticky;top:0;background:#0e1116}}
td{{padding:10px 8px;border-bottom:1px solid #1c2027}}
tr:hover td{{background:#161b22}}
.rk{{color:#9aa0a6;width:36px}} .tk{{font-weight:700;font-size:15px}}
.nm{{color:#9aa0a6;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.vol{{font-weight:700}} .up{{color:#34d399}} .dn{{color:#f87171}}
.note{{color:#9aa0a6;font-size:12px;margin-top:16px;line-height:1.6}}
</style></head><body>
<h1>🔍 Highest options volume — {stamp}</h1>
<div class="sub">{scanned} tickers scanned · {failed} without usable data · ranked by total call+put volume</div>
<table><thead><tr><th>#</th><th>Ticker</th><th>Name</th><th>Price</th><th>Day</th>
<th>Options Vol</th><th>Calls</th><th>Puts</th><th>P/C</th></tr></thead>
<tbody>{body}</tbody></table>
<div class="note">Methodology: sums traded volume across the nearest {n_expiries} option
expirations per underlying (where weekly options concentrate the bulk of volume), using
15-minute-delayed Yahoo Finance data. 0DTE index options (SPX) and OTC names are not covered.
Run after the close for final daily numbers; intraday runs show volume so far.</div>
</body></html>"""


def get_light_universe(top_n=80):
    """Get top N tickers by options volume from the last full scan.

    Used for --light scans: only re-scan the tickers that matter most,
    cutting API calls by ~75%. Falls back to full TICKERS if no prior
    full scan exists.
    """
    # Find the most recent full scan (options_volume.json, not light)
    pattern = os.path.join(REPORTS, "*", "options_volume.json")
    candidates = sorted(glob.glob(pattern))
    # Exclude light files (they have _light in the name, but our pattern
    # only matches options_volume.json exactly, so this is safe)
    if not candidates:
        print("No prior full scan found, using full universe", flush=True)
        return TICKERS
    latest = candidates[-1]
    try:
        with open(latest) as f:
            data = json.load(f)
        rows = data.get("rows", [])
        # Sort by options_volume descending, take top N
        rows.sort(key=lambda r: r.get("options_volume", 0), reverse=True)
        tickers = [r["ticker"] for r in rows[:top_n] if r.get("ticker")]
        if len(tickers) < 10:
            print(f"Prior scan had only {len(tickers)} tickers, using full universe",
                  flush=True)
            return TICKERS
        print(f"Light universe: top {len(tickers)} from {latest}", flush=True)
        return tickers
    except Exception as ex:
        print(f"Failed to read {latest} ({ex}), using full universe", flush=True)
        return TICKERS


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--expiries", type=int, default=6)
    ap.add_argument("--workers", type=int, default=5)
    ap.add_argument("--top", type=int, default=60)
    ap.add_argument("--light", action="store_true",
                    help="Light scan: only top 80 tickers by last full-scan volume. "
                         "Writes to options_volume_light.json. Cuts API calls ~75%.")
    ap.add_argument("--light-top", type=int, default=80,
                    help="Number of top tickers for --light scans (default 80)")
    args = ap.parse_args()

    stamp = datetime.date.today().isoformat()
    if not is_trading_day():
        print(f"{stamp} is not a trading day (weekend/holiday) — skipping.")
        return
    day_dir = os.path.join(REPORTS, stamp)
    os.makedirs(day_dir, exist_ok=True)

    # Determine universe: full (311) or light (top 80)
    if args.light:
        universe = get_light_universe(top_n=args.light_top)
        is_light = True
        output_json = os.path.join(day_dir, "options_volume_light.json")
        output_csv = os.path.join(day_dir, "options_volume_light.csv")
    else:
        universe = TICKERS
        is_light = False
        output_json = os.path.join(day_dir, "options_volume.json")
        output_csv = os.path.join(day_dir, "options_volume.csv")

    # Bulk price fetch: 1-2 Yahoo calls for all tickers instead of
    # individual fast_info calls. Saves ~12% of API calls and ~50s.
    print(f"Fetching bulk prices for {len(universe)} tickers...", flush=True)
    price_cache = fetch_bulk_prices(universe)
    print(f"bulk prices: {len(price_cache)}/{len(universe)} tickers", flush=True)

    # Track API calls for throttling monitor
    api_calls = {"count": 0}
    orig_call = _call
    def counted_call(fn):
        api_calls["count"] += 1
        return orig_call(fn)
    # Monkey-patch for this run (scan_one uses _call via closure)
    import scan as _self
    _self._call = counted_call

    rows, failed = [], 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(scan_one, s, args.expiries, price_cache): s
                for s in universe}
        for f in concurrent.futures.as_completed(futs):
            r = f.result()
            if r:
                rows.append(r)
            else:
                failed += 1

    # Restore original _call
    _self._call = orig_call

    rows.sort(key=lambda r: r["options_volume"], reverse=True)
    rows = fetch_names(rows, limit=args.top)

    df = pd.DataFrame(rows)
    df.to_csv(output_csv, index=False)
    with open(output_json, "w") as fh:
        json.dump({"date": stamp, "scanned": len(universe), "failed": failed,
                   "rows": rows, "light": is_light,
                   "api_calls": api_calls["count"]}, fh, indent=1)

    # Bank daily totals for the unusual-activity ranking (idempotent per date).
    # Light scans do NOT update history.csv — they're partial intraday snapshots,
    # not the full-universe daily totals.
    if not is_light:
        hist = pd.DataFrame([{"date": stamp, "ticker": r["ticker"],
                              "options_volume": r["options_volume"]} for r in rows])
        if os.path.exists(HISTORY_CSV):
            old = pd.read_csv(HISTORY_CSV)
            old = old[old["date"] != stamp]
            hist = pd.concat([old, hist], ignore_index=True)
        hist.to_csv(HISTORY_CSV, index=False)
        with open(os.path.join(day_dir, "report.html"), "w") as fh:
            fh.write(html_report(rows, stamp, len(universe), failed, args.expiries))

    scan_type = "LIGHT" if is_light else "FULL"
    print(f"[{scan_type}] scanned={len(universe)} ok={len(rows)} failed={failed} "
          f"api_calls={api_calls['count']} rate_limit_hits={_rate_limit_hits} "
          f"-> {output_json}")
    # THROTTLING ALERT: If Yahoo is rate-limiting us, this needs attention.
    # >10 hits per scan means we're pushing the limit; >20 means we're likely
    # to get IP-blocked soon.
    if _rate_limit_hits > 10:
        print(f"WARNING: Yahoo throttling detected ({_rate_limit_hits} rate-limit hits). "
              f"Consider reducing scan frequency or universe size.", flush=True)
    for i, r in enumerate(rows[:20], 1):
        print(f"{i:2d}. {r['ticker']:6s} {r['options_volume']:>12,}  "
              f"${r['price']:>9,.2f} {r['chg_pct']:+.2f}%  {r.get('name','')}")


if __name__ == "__main__":
    main()
