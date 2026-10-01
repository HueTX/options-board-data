#!/usr/bin/env python3
"""Fetch live Yahoo 1-minute chart data for a ticker list and publish a compact
quotes JSON file. Designed to run on GitHub Actions every few minutes during
market hours. Stdlib + requests (the workflow pip-installs requests because
actions/setup-python's Python does not bundle it).

Usage:
    python fetch_quotes.py tickers_priority.txt quotes.json
    python fetch_quotes.py tickers_universe.txt quotes_universe.json
    python fetch_quotes.py --movers market_movers.json   # whole-market day gainers/losers
    python fetch_quotes.py --watchlist watchlist.json   # morning watchlist levels, once/day
"""
import json
import os
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


def fetch_movers_screener(sess, scr_id, count=50, tries=3):
    """Pull one Yahoo predefined screener (day_gainers / day_losers)."""
    url = ("https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
           "?scrIds=%s&count=%d" % (scr_id, count))
    delay = 1.0
    last = None
    for _ in range(tries):
        try:
            r = sess.get(url, timeout=15)
            if r.status_code == 200:
                j = r.json()
                quotes = ((j.get("finance") or {}).get("result") or [{}])[0].get("quotes") or []
                out = []
                for q in quotes:
                    sym = q.get("symbol")
                    px = q.get("regularMarketPrice")
                    chg = q.get("regularMarketChangePercent")
                    if sym and isinstance(px, (int, float)) and isinstance(chg, (int, float)):
                        entry = {"t": sym, "price": r4(px), "chgPct": r4(chg)}
                        mc = q.get("marketCap")
                        if isinstance(mc, (int, float)):
                            entry["mc"] = int(mc)  # raw dollars; entries without one sort last
                        out.append(entry)
                return out
            last = "http %d" % r.status_code
        except Exception as e:
            last = str(e)[:80]
        time.sleep(delay)
        delay *= 2
    raise RuntimeError("%s failed: %s" % (scr_id, last))


def fetch_movers(out_file):
    """Write market_movers.json: whole-market day gainers/losers from Yahoo's
    predefined screeners (NOT limited to our tracked universe). Each entry
    carries t/price/chgPct plus mc (marketCap, raw dollars) when Yahoo returns it.
    Falls back to ranking the locally fetched 311-ticker universe when the
    screeners are unreachable, so the board keeps a fresh movers feed."""
    sess = requests.Session()
    sess.headers.update(UA)
    gainers, losers = [], []
    try:
        gainers = fetch_movers_screener(sess, "day_gainers")
    except Exception as e:
        print("day_gainers: %s" % e)
    try:
        losers = fetch_movers_screener(sess, "day_losers")
    except Exception as e:
        print("day_losers: %s" % e)
    if not gainers and not losers:
        # Screener blocked/failed: rank the universe quotes file this job
        # already maintains instead of publishing nothing.
        try:
            with open("quotes_universe.json") as f:
                uni = json.load(f).get("tickers", {})
            scored = []
            for sym, e in uni.items():
                p, pc = e.get("p"), e.get("pc")
                if isinstance(p, (int, float)) and isinstance(pc, (int, float)) and pc:
                    scored.append({"t": sym, "price": r4(p),
                                   "chgPct": r4((p - pc) / pc * 100)})
            if scored:
                gainers = sorted([s for s in scored if s["chgPct"] > 0],
                                 key=lambda x: -x["chgPct"])[:50]
                losers = sorted([s for s in scored if s["chgPct"] < 0],
                                key=lambda x: x["chgPct"])[:50]
                print("movers: screener failed, ranked %d universe tickers locally" % len(scored))
        except Exception as e:
            print("movers local fallback failed: %s" % str(e)[:80])
    if not gainers and not losers:
        # Don't clobber the last good file with an empty one; the workflow
        # treats a nonzero exit as "keep previous".
        print("market movers: both screeners failed, not writing %s" % out_file)
        sys.exit(1)
    gainers.sort(key=lambda x: x["chgPct"], reverse=True)
    losers.sort(key=lambda x: x["chgPct"])
    payload = {
        "asOf": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "gainers": gainers[:50],
        "losers": losers[:50],
    }
    tmp = out_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    import os
    os.replace(tmp, out_file)
    print("wrote %s: %d gainers, %d losers" % (out_file, len(gainers), len(losers)))


def fetch_chart(sess, sym, rng, interval, tries=3):
    url = ("https://query1.finance.yahoo.com/v8/finance/chart/%s"
           "?interval=%s&range=%s&includePrePost=true" % (sym, interval, rng))
    delay = 1.0
    for _ in range(tries):
        try:
            r = sess.get(url, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503):
                time.sleep(delay)
                delay *= 2
                continue
            return None
        except Exception:
            time.sleep(delay)
            delay *= 2
    return None


def build_watchlist(out_file, tickers_file="watchlist_tickers.txt",
                    caps_file="watchlist_caps.json"):
    """DEPRECATED as a builder: the 08:05 CT morning-breakout-watchlist cron is
    now the SOLE writer of watchlist.json (true 311-ticker premarket scan).
    Rebuilding here ranked a different 72-ticker set by closing-price proximity
    and clobbered the real morning list on every workflow run (2026-10-01).
    This function is kept only so the workflow's --watchlist step stays green;
    it never writes. To restore a local rebuild, reimplement from the morning
    scan's state file, not from this ticker subset."""
    print("watchlist: owned by the morning-breakout-watchlist cron, not rebuilding here")
    return
    # --- retired implementation below (kept for reference, unreachable) ---
    _retired_build_watchlist(out_file, tickers_file, caps_file)


def _retired_build_watchlist(out_file, tickers_file="watchlist_tickers.txt",
                             caps_file="watchlist_caps.json"):
    """Write watchlist.json once per trading day: PDH/PDL from the last
    completed daily bar, PMH/PML from today's premarket 1-minute bars
    (before 09:30 ET), ranked by proximity of the live price to the nearest
    level, top 22. Runs after the premarket completes; keeps the previous
    file on any failure so the board never goes blank."""
    import os
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    now_et = datetime.datetime.now(datetime.timezone.utc).astimezone(ET)
    today = now_et.date().isoformat()
    if os.path.exists(out_file):
        try:
            if json.load(open(out_file)).get("marketDate") == today:
                print("watchlist already built for %s, skipping" % today)
                return
        except Exception:
            pass
    if now_et.hour * 60 + now_et.minute < 9 * 60 + 35:
        print("watchlist: premarket not complete yet (%s ET), skipping" % now_et.strftime("%H:%M"))
        return
    with open(tickers_file) as f:
        tickers = [l.strip() for l in f if l.strip()]
    caps = {}
    if os.path.exists(caps_file):
        try:
            caps = json.load(open(caps_file))
        except Exception:
            pass

    sess = requests.Session()
    sess.headers.update(UA)
    try:
        sess.get("https://query1.finance.yahoo.com/v8/finance/chart/SPY?interval=1m&range=1d",
                 timeout=10)
    except Exception:
        pass

    def one(sym):
        j1 = fetch_chart(sess, sym, "1d", "1m")
        j2 = fetch_chart(sess, sym, "2d", "1d")
        if not j1 or not j2:
            return sym, None
        try:
            r1 = j1["chart"]["result"][0]
            r2 = j2["chart"]["result"][0]
        except Exception:
            return sym, None
        # Daily bars: last bar must be today (else market closed / no data).
        dts = r2.get("timestamp") or []
        dq = ((r2.get("indicators") or {}).get("quote") or [{}])[0] or {}
        d_days = [datetime.datetime.fromtimestamp(t, ET).date().isoformat() for t in dts]
        if not d_days or d_days[-1] != today or len(dts) < 1:
            return sym, None
        prev = dts.index(dts[-2]) if len(dts) >= 2 else 0
        pdh = (dq.get("high") or [None])[prev]
        pdl = (dq.get("low") or [None])[prev]
        # 1m bars: premarket = today before 09:30 ET.
        ts = r1.get("timestamp") or []
        q = ((r1.get("indicators") or {}).get("quote") or [{}])[0] or {}
        hs, ls = q.get("high") or [], q.get("low") or []
        pmh = pml = None
        has_today = False
        for i, t in enumerate(ts):
            dt = datetime.datetime.fromtimestamp(t, ET)
            if dt.date().isoformat() != today:
                continue
            has_today = True
            if dt.hour * 60 + dt.minute < 9 * 60 + 30:
                h, l = hs[i] if i < len(hs) else None, ls[i] if i < len(ls) else None
                if h is not None:
                    pmh = h if pmh is None else max(pmh, h)
                if l is not None:
                    pml = l if pml is None else min(pml, l)
        if not has_today:
            return sym, None
        m = r1.get("meta") or {}
        price = m.get("regularMarketPrice")
        if price is None:
            cl = q.get("close") or []
            price = cl[-1] if cl else None
        if price is None:
            return sym, None
        pc = m.get("chartPreviousClose", m.get("previousClose"))
        chg = round((price - pc) / pc * 100, 2) if pc else None
        levels = {"PDH": pdh, "PDL": pdl, "PMH": pmh, "PML": pml}
        best, bdist = None, None
        for nm, lv in levels.items():
            if lv is None:
                continue
            d = abs(price - lv) / price * 100
            if bdist is None or d < bdist:
                best, bdist = nm, d
        return sym, {"ticker": sym, "pdh": r4(pdh), "pdl": r4(pdl),
                     "pmh": r4(pmh), "pml": r4(pml), "refPx": r4(price),
                     "nearName": best, "nearDist": round(bdist, 2) if bdist is not None else None,
                     "marketCapB": caps.get(sym), "changePct": chg}

    rows = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for sym, row in ex.map(one, tickers):
            if row:
                rows.append(row)
    if len(rows) < 15:
        # Too few to be trustworthy — keep the previous file.
        print("watchlist: only %d/%d tickers ok, not writing %s" % (len(rows), len(tickers), out_file))
        sys.exit(1)
    rows.sort(key=lambda r: (r["nearDist"] if r["nearDist"] is not None else 999, r["ticker"]))
    rows = rows[:22]
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    payload = {"marketDate": today,
               "builtAt": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "rows": rows}
    tmp = out_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    os.replace(tmp, out_file)
    print("wrote %s: %d rows for %s" % (out_file, len(rows), today))


def build_dbars(out_file, snapshot_file="snapshot.json",
                movers_file="market_movers.json",
                priority_file="tickers_priority.txt"):
    """Drawer bars: 5-minute intraday bars (+ quote fields) for every ticker
    that can open the detail drawer but is NOT in the 56-ticker quotes.json
    feed — the current snapshot flow rows (bullish/bearish/premarket) and the
    market-movers gainers/losers. The board ingests these into the same quote
    map, so the drawer line chart, day range, volume and premarket H/L all
    work for tickers like APP that quotes.json does not cover. 5m bars are
    ~5x smaller than quotes.json's 1m bars and plenty for the drawer chart.
    On too many failures the previous file is kept (never a blank board)."""
    syms = []

    def add_from(path, key_fn):
        try:
            d = json.load(open(path))
        except Exception as e:
            print("dbars: cannot read %s (%s)" % (path, e))
            return
        for s in key_fn(d) or []:
            if s and s not in syms:
                syms.append(s)

    add_from(snapshot_file, lambda d: [r.get("ticker") for r in
             (d.get("bullish") or []) + (d.get("bearish") or []) + (d.get("premarket") or [])])
    add_from(movers_file, lambda d: [m.get("t") for m in
             (d.get("gainers") or []) + (d.get("losers") or [])])
    try:
        pri = set(l.strip() for l in open(priority_file) if l.strip())
    except Exception:
        pri = set()
    syms = [s for s in syms if s not in pri]
    if not syms:
        print("dbars: no tickers to fetch, keeping previous %s" % out_file)
        return
    sess = requests.Session()
    sess.headers.update(UA)
    try:
        sess.get("https://query1.finance.yahoo.com/v8/finance/chart/SPY?interval=1m&range=1d",
                 timeout=10)
    except Exception:
        pass
    out, ok = {}, 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_chart, sess, s, "1d", "5m"): s for s in syms}
        for fut in futs:
            s = futs[fut]
            try:
                j = fut.result()
            except Exception:
                j = None
            if j:
                c = compact(s, j)
                if c and c.get("bars"):
                    out[s] = c
                    ok += 1
    if ok < max(5, len(syms) // 4):
        print("dbars: only %d/%d tickers ok, keeping previous %s" % (ok, len(syms), out_file))
        sys.exit(1)
    payload = {"asOf": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "count": ok, "bars": out}
    tmp = out_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    os.replace(tmp, out_file)
    print("wrote %s: %d tickers" % (out_file, ok))


def build_regime1h(out_file):
    """1-hour regime bars: last 107 regular-session hourly closes for
    SPY/QQQ/IWM, built with the exact semantics of wcharts.json's h1 series
    (Yahoo 1h bars, regular session 09:30-16:00 ET only, premarket excluded,
    closes rounded to 2dp). The board merges these into the watchlist chart
    map so the regime-strip sparklines plot the same 1-hour bars as the
    trading-morning watchlist. On failure the previous file is kept."""
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    sess = requests.Session()
    sess.headers.update(UA)
    charts = {}
    for sym in ("SPY", "QQQ", "IWM"):
        j = fetch_chart(sess, sym, "6mo", "1h")
        h1 = []
        try:
            r = j["chart"]["result"][0]
            ts = r.get("timestamp") or []
            q = ((r.get("indicators") or {}).get("quote") or [{}])[0] or {}
            closes = q.get("close") or []
            bars = []
            now_ts = time.time()
            for t, c in zip(ts, closes):
                if c is None:
                    continue
                et = datetime.datetime.fromtimestamp(t, datetime.timezone.utc).astimezone(ET)
                m = et.hour * 60 + et.minute
                # Match the artifact watchlist parser: only completed hourly
                # candles whose start is inside the 09:30–16:00 ET session.
                if et.weekday() < 5 and 570 <= m < 960 and t + 3600 <= now_ts:
                    bars.append(round(float(c), 2))
            h1 = bars[-107:]
        except Exception as e:
            print("regime1h: %s parse failed (%s)" % (sym, e))
        print("regime1h: %s h1=%d" % (sym, len(h1)))
        if len(h1) >= 20:
            charts[sym] = {"h1": h1}
    if len(charts) < 3:
        print("regime1h: only %d/3 ok, keeping previous %s" % (len(charts), out_file))
        sys.exit(1)
    payload = {"asOf": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "charts": charts}
    tmp = out_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    os.replace(tmp, out_file)
    print("wrote %s" % out_file)


def main():
    if "--watchlist" in sys.argv:
        rest = [a for a in sys.argv[1:] if a != "--watchlist"]
        build_watchlist(rest[0] if rest else "watchlist.json")
        return
    if "--dbars" in sys.argv:
        rest = [a for a in sys.argv[1:] if a != "--dbars"]
        build_dbars(rest[0] if rest else "dbars.json")
        return
    if "--regime1h" in sys.argv:
        rest = [a for a in sys.argv[1:] if a != "--regime1h"]
        build_regime1h(rest[0] if rest else "regime1h.json")
        return
    if "--movers" in sys.argv:
        rest = [a for a in sys.argv[1:] if a != "--movers"]
        fetch_movers(rest[0] if rest else "market_movers.json")
        return
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
