#!/usr/bin/env python3
"""Premarket runners builder (runs every 15 min premarket on trading days).

Scans the FULL universe for today's premarket movers: pulls live premarket
quotes for every ticker and reports those gapping +/-GAP_PCT vs the prior
close. Yesterday's unusual-options-flow stats are attached as enrichment
where available, but any ticker gapping in today's premarket qualifies --
the list is today's runners, not yesterday's flow names.
Writes premarket.json, which the premarket-watchlist cron pushes to the board.
"""
import csv, json, os, glob, datetime, time

import yfinance as yf

import universe

BASE = os.path.dirname(os.path.abspath(__file__))
BASELINES = os.path.join(BASE, "baselines_mc.csv")
BASELINES_90D = os.path.join(BASE, "baselines_90d.csv")
BASELINES_10D = os.path.join(BASE, "baselines_mc10.csv")
OUT = os.path.join(BASE, "premarket.json")

GAP_PCT = 2.0   # |premarket % move| that counts as gapping
MAX_GAPS = 15   # runners shown, ranked by |gap|
CHUNK = 50      # tickers per yfinance batch (keeps batch failures low)
PM_OPEN = datetime.time(4, 0)    # premarket window (ET)
PM_CUTOFF = datetime.time(9, 30)


def pc_lean(pc):
    if not pc:
        return ""
    if pc < 0.7:
        return "calls leaning"
    if pc > 1.3:
        return "puts leaning"
    return "mixed flow"


def load_caps():
    """ticker -> market cap in $B from the weekly-refreshed marketcaps.csv."""
    m = {}
    try:
        with open(os.path.join(BASE, "marketcaps.csv")) as fh:
            for row in csv.DictReader(fh):
                v = (row.get("marketCapB") or "").strip()
                m[row["ticker"].strip().upper()] = float(v) if v else None
    except FileNotFoundError:
        pass
    return m


def load_baselines():
    bl = {}
    path = BASELINES_90D if os.path.exists(BASELINES_90D) else BASELINES
    with open(path) as fh:
        for row in csv.DictReader(fh):
            for key in ("avg90_volume", "mc_avg90_opt_vol", "avg_volume", "avg10_volume"):
                if row.get(key):
                    try:
                        sym = row.get("symbol") or row.get("ticker")
                        if sym:
                            bl[sym] = int(row[key]); break
                    except ValueError:
                        pass
    return bl


def close_series(data, t):
    """data['Close'][t] whether columns are multi-index or single."""
    c = data["Close"]
    if hasattr(c, "columns") and t in c.columns:
        return c[t].dropna()
    return c.dropna()


def main():
    tickers = list(universe.TICKERS)

    # Yesterday's flow stats, for enrichment only (never a filter).
    flow = {}
    ranked = []
    days = sorted(glob.glob(os.path.join(BASE, "reports", "*", "options_volume.json")))
    if days:
        d = json.load(open(days[-1]))
        bl = load_baselines()
        for r in d["rows"]:
            avg = bl.get(r["ticker"])
            if not avg:
                continue
            e = {"ticker": r["ticker"], "rel": r["options_volume"] / avg,
                 "chg": r["chg_pct"], "pc": r.get("pc_ratio", 0)}
            flow[r["ticker"]] = e
            ranked.append(e)
        ranked.sort(key=lambda e: -e["rel"])
    day = os.path.basename(os.path.dirname(days[-1])) if days else ""

    # Today's premarket movers across the full universe.
    # The premarket session date comes from the bars themselves, and the
    # reference day is the last completed daily bar BEFORE that date, so the
    # gap is always measured vs the correct prior close (no off-by-one).
    gaps = []
    pm_date = None
    caps = load_caps()
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i:i + CHUNK]
        try:
            data = yf.download(" ".join(chunk), period="1d", interval="1m",
                               prepost=True, progress=False, auto_adjust=False,
                               threads=True)
            daily = yf.download(" ".join(chunk), period="5d", interval="1d",
                                progress=False, auto_adjust=False, threads=True)
        except Exception as ex:
            print(f"premarket chunk {i // CHUNK + 1} failed: {ex}")
            continue
        for t in chunk:
            try:
                s = close_series(data, t)
                if len(s) == 0:
                    continue
                idx = s.index
                pmask = ((idx.time >= PM_OPEN) & (idx.time < PM_CUTOFF))
                pm = s[pmask]
                if len(pm) == 0:
                    continue
                last = float(pm.iloc[-1])
                pmd = pm.index[-1].date()
                pm_time = pm.index[-1].isoformat()
                if pm_date is None:
                    pm_date = pmd
                # reference day: last daily bar strictly before the premarket date
                dc = close_series(daily, t)
                dh = daily["High"][t].dropna() if t in daily["High"].columns else daily["High"].dropna()
                dl = daily["Low"][t].dropna() if t in daily["Low"].columns else daily["Low"].dropna()
                dmask = dc.index.date < pmd
                if dmask.sum() < 2:
                    continue
                ref_close = float(dc[dmask].iloc[-1])
                ref_high = float(dh[dmask].iloc[-1])
                ref_low = float(dl[dmask].iloc[-1])
                prev2_close = float(dc[dmask].iloc[-2])
                if ref_close <= 0:
                    continue
                gap = (last / ref_close - 1) * 100
                if abs(gap) >= GAP_PCT:
                    if last > ref_high:
                        pos = "above"
                    elif last < ref_low:
                        pos = "below"
                    else:
                        pos = "inside"
                    f = flow.get(t)
                    gaps.append({
                        "ticker": t, "gap": round(gap, 1),
                        "pm_price": round(last, 2),
                        "pm_time": pm_time,
                        "marketCapB": caps.get(t),
                        "rel": round(f["rel"], 1) if f else None,
                        "pc": f["pc"] if f else None,
                        "lean": pc_lean(f["pc"]) if f else "",
                        "yday": dc[dmask].index[-1].strftime("%a"),
                        "ychg": round((ref_close / prev2_close - 1) * 100, 1),
                        "ph": round(ref_high, 2), "pl": round(ref_low, 2),
                        "pos": pos,
                    })
            except Exception:
                continue
        time.sleep(1)
    gaps.sort(key=lambda g: -abs(g["gap"]))
    gaps = gaps[:MAX_GAPS]

    heaters = [{"ticker": c["ticker"], "rel": round(c["rel"], 1),
                "chg": c["chg"], "pc": c["pc"]} for c in ranked[:15]]

    payload = {"date": pm_date.strftime("%Y-%m-%d") if pm_date else day,
               "generated_at": datetime.datetime.now().isoformat(timespec="minutes"),
               "scanned": len(tickers),
               "gaps": gaps, "heaters": heaters}
    json.dump(payload, open(OUT, "w"), indent=1)
    print(f"premarket runners -> {OUT}  scanned={len(tickers)} gaps={len(gaps)} heaters={len(heaters)}")
    for g in gaps:
        fl = f"  flow {g['rel']}x" if g["rel"] else ""
        print(f"  {g['ticker']:6s} {g['gap']:+.1f}% @ {g['pm_price']}{fl}")


if __name__ == "__main__":
    main()
