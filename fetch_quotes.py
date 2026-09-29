#!/usr/bin/env python3
"""Fetch live Yahoo 1-minute chart data for a ticker list and publish a compact
quotes JSON file. Designed to run on GitHub Actions every few minutes during
market hours. Stdlib + requests only (requests is preinstalled on GH runners).

Usage:
    python fetch_quotes.py tickers_priority.txt quotes.json
    python fetch_quotes.py tickers_universe.txt quotes_universe.json
"""
import json
import sys
import time
import datetime
from concurrent.futures import ThreadPoolExecutor

import requests

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def fetch_one(sess, sym, tries=4):
    url = ("https://query1.finance.yahoo.com/v8/finance/chart/%s"
           "?interval=1m&range=1d&includePrePost=true" % sym)
    delay = 1.0
    for _ in range(tries):
        try:
            r = sess.get(url, timeout=12)
            if r.status_code == 200:
                return sym, r.json()
            if r.status_code in (429, 500, 502, 503):
                time.sleep(delay)
                delay *= 2
                continue
            return sym, None
        except Exception:
            time.sleep(delay)
            delay *= 2
    return sym, None


def r4(x):
    return round(x, 4) if isinstance(x, (int, float)) else None


def compact(sym, j):
    try:
        r = j["chart"]["result"][0]
    except Exception:
        return None
    m = r.get("meta") or {}
    ts = r.get("timestamp") or []
    ind = r.get("indicators") or {}
    q = (ind.get("quote") or [{}])[0] or {}
    opens = q.get("open") or []
    highs = q.get("high") or []
    lows = q.get("low") or []
    closes = q.get("close") or []

    def at(arr, i):
        return arr[i] if i < len(arr) else None

    bars = []
    for i, t in enumerate(ts):
        c = at(closes, i)
        if c is None:
            continue
        o = at(opens, i)
        h = at(highs, i)
        l = at(lows, i)
        bars.append([int(t), r4(o if o is not None else c), r4(h if h is not None else c),
                     r4(l if l is not None else c), r4(c)])
    if not bars:
        return None
    price = m.get("regularMarketPrice")
    if price is None:
        price = bars[-1][4]
    prev_close = m.get("chartPreviousClose", m.get("previousClose"))
    return {
        "p": r4(price),
        "pc": r4(prev_close),
        "h": r4(m.get("regularMarketDayHigh")),
        "l": r4(m.get("regularMarketDayLow")),
        "v": m.get("regularMarketVolume"),
        "t": m.get("regularMarketTime"),
        "g": m.get("gmtoffset"),
        "bars": bars,
    }


def main():
    tickers_file = sys.argv[1] if len(sys.argv) > 1 else "tickers_priority.txt"
    out_file = sys.argv[2] if len(sys.argv) > 2 else "quotes.json"
    no_bars = "--no-bars" in sys.argv
    with open(tickers_file) as f:
        tickers = [l.strip() for l in f if l.strip()]

    sess = requests.Session()
    sess.headers.update(UA)
    # warm up cookies once (helps Yahoo accept the burst)
    try:
        sess.get("https://query1.finance.yahoo.com/v8/finance/chart/SPY?interval=1m&range=1d",
                 timeout=10)
    except Exception:
        pass

    out = {}
    ok = 0
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = [ex.submit(fetch_one, sess, s) for s in tickers]
        for fut in futs:
            sym, j = fut.result()
            if j:
                c = compact(sym, j)
                if c:
                    if no_bars:
                        c.pop("bars", None)
                    out[sym] = c
                    ok += 1
    payload = {
        "asOf": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tickers": out,
    }
    tmp = out_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    import os
    os.replace(tmp, out_file)
    print("wrote %s: %d/%d tickers ok" % (out_file, ok, len(tickers)))


if __name__ == "__main__":
    main()
