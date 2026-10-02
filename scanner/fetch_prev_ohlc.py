"""Fetch previous trading day high/low per ticker, cached per date.

Extracted from live_board.py::prev_ohlc for the GitHub Actions pipeline.
Writes prev_ohlc_<YYYY-MM-DD>.json (today's date) in the script directory.
Idempotent: skips the fetch if today's cache file already exists.
"""
import datetime
import json
import os
import sys

import yfinance as yf

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from universe import TICKERS  # noqa: E402


def main():
    today = datetime.date.today().isoformat()
    cache = os.path.join(BASE, f"prev_ohlc_{today}.json")
    if os.path.exists(cache):
        print(f"prev_ohlc cache exists: {cache}, skipping")
        return
    tickers = sorted(set(TICKERS))
    out = {}
    try:
        data = yf.download(" ".join(tickers), period="5d", interval="1d",
                           progress=False, auto_adjust=False, threads=True)
        for t in tickers:
            try:
                hi = data["High"][t].dropna()
                lo = data["Low"][t].dropna()
                if len(hi) >= 2 and len(lo) >= 2:
                    out[t] = {"prev_high": round(float(hi.iloc[-2]), 2),
                              "prev_low": round(float(lo.iloc[-2]), 2)}
            except Exception:
                continue
    except Exception as e:
        print(f"prev_ohlc fetch failed: {e}", file=sys.stderr)
    if out:
        json.dump(out, open(cache, "w"))
        print(f"wrote {cache} ({len(out)} tickers)")
    else:
        print("prev_ohlc: no data fetched, not caching", file=sys.stderr)


if __name__ == "__main__":
    main()
