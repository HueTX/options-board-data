#!/usr/bin/env python3
"""
build_persistence.py — Flow Persistence Score, Confirmed Ignitions, First-Appearance,
Sector Rotation, Flow Echo.

Reads:
  scanner/history.csv            — date,ticker,options_volume (daily bank)
  scanner/baselines_90d.csv      — symbol,avg90_volume (unusual-flow baseline)
  scanner/sectors.csv            — ticker,sector
  snapshot.json                  — today's bullish/bearish flow + prices
  follow_through.json            — yesterday's ignitions [{ticker, x}]
  fifteen_min_closes.json        — today's 15m confirmed closes {bullish[], bearish[]}

Writes:
  persistence.json — {
    asOf, marketDate,
    streaks: [{ticker, side, streakDays, lastX, priceFollow}],
    confirmed: [{ticker, ignitionX, breakoutSide, breakoutCandle}],
    firstAppearance: [{ticker, side, x}],
    sectorRotation: [{sector, thisWeekPct, lastWeekPct, deltaPct}],
    echo: [{ticker, side, x, lastSeenDaysAgo}]
  }

Unusual = options_volume >= 2x the 90d baseline (matches the board's gold threshold).
"""
import csv, json, os, sys
from collections import defaultdict
from datetime import datetime, timezone

SCAN = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(SCAN)  # repo root when run from scanner/, else override

def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None

def main():
    out_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "persistence.json")
    # data files live at repo root (published) or scanner/ dir (local)
    def find(name):
        for p in (os.path.join(ROOT, name), os.path.join(SCAN, name)):
            if os.path.exists(p):
                return p
        return None

    # --- baselines ---
    baselines = {}
    bp = find("baselines_90d.csv") or os.path.join(SCAN, "baselines_90d.csv")
    with open(bp) as f:
        for row in csv.DictReader(f):
            try:
                baselines[row["symbol"].strip().upper()] = float(row["avg90_volume"])
            except (ValueError, KeyError):
                pass

    # --- history: date -> ticker -> volume ---
    hist = defaultdict(dict)
    hp = find("history.csv") or os.path.join(SCAN, "history.csv")
    with open(hp) as f:
        for row in csv.DictReader(f):
            try:
                hist[row["date"]][row["ticker"].strip().upper()] = float(row["options_volume"])
            except (ValueError, KeyError):
                pass
    dates = sorted(hist.keys())
    today = dates[-1] if dates else None

    def is_unusual(ticker, date):
        base = baselines.get(ticker)
        vol = hist.get(date, {}).get(ticker)
        if not base or not vol or base <= 0:
            return False, 0.0
        x = vol / base
        return x >= 2.0, x

    # --- today's snapshot for side + price follow-through ---
    snap = load_json(find("snapshot.json") or "") or {}
    market_date = snap.get("marketDate") or today
    side_of = {}
    px_chg = {}
    for side, key in (("bull", "bullish"), ("bear", "bearish")):
        for r in snap.get(key, []) or []:
            t = (r.get("ticker") or "").upper()
            if t:
                side_of[t] = side
                px_chg[t] = r.get("changePct")

    # --- 1. streaks: consecutive unusual days ending today ---
    streaks = []
    for ticker in hist.get(today, {}):
        x_today, mult = is_unusual(ticker, today)
        if not x_today:
            continue
        streak = 1
        for d in reversed(dates[:-1]):
            u, _ = is_unusual(ticker, d)
            if u:
                streak += 1
            else:
                break
        side = side_of.get(ticker, "bull" if (px_chg.get(ticker) or 0) >= 0 else "bear")
        chg = px_chg.get(ticker)
        price_follow = None
        if chg is not None:
            price_follow = (chg >= 0) if side == "bull" else (chg <= 0)
        streaks.append({
            "ticker": ticker, "side": side, "streakDays": streak,
            "lastX": round(mult, 2), "priceFollow": price_follow,
        })
    streaks.sort(key=lambda r: (-r["streakDays"], -r["lastX"]))

    # --- 2. confirmed ignitions: yesterday's ignition + today's 15m breakout ---
    ft = load_json(find("follow_through.json") or "") or {}
    ignitions = { (i.get("ticker") or "").upper(): i.get("x") for i in ft.get("ignitions", []) or [] }
    closes = load_json(find("fifteen_min_closes.json") or "") or {}
    confirmed = []
    for side, key in (("bull", "bullish"), ("bear", "bearish")):
        for r in closes.get(key, []) or []:
            t = (r.get("ticker") or "").upper()
            if t in ignitions:
                confirmed.append({
                    "ticker": t, "ignitionX": ignitions[t],
                    "breakoutSide": side, "breakoutCandle": r.get("candle"),
                })
    confirmed.sort(key=lambda r: -(r["ignitionX"] or 0))

    # --- 3. first appearance: unusual today, not unusual in prior 30 days ---
    first = []
    lookback = dates[-31:-1] if len(dates) > 1 else []
    for ticker in hist.get(today, {}):
        x_today, mult = is_unusual(ticker, today)
        if not x_today:
            continue
        if any(is_unusual(ticker, d)[0] for d in lookback):
            continue
        side = side_of.get(ticker, "bull" if (px_chg.get(ticker) or 0) >= 0 else "bear")
        first.append({"ticker": ticker, "side": side, "x": round(mult, 2)})
    first.sort(key=lambda r: -r["x"])

    # --- 4. sector rotation: this week's unusual-flow share vs last week's ---
    sectors = {}
    sp = os.path.join(SCAN, "sectors.csv")
    if os.path.exists(sp):
        with open(sp) as f:
            for row in csv.DictReader(f):
                sectors[row["ticker"].strip().upper()] = row["sector"]
    def week_share(week_dates):
        tot, by_sec = 0.0, defaultdict(float)
        for d in week_dates:
            for t, vol in hist.get(d, {}).items():
                u, _ = is_unusual(t, d)
                if not u:
                    continue
                sec = sectors.get(t, "Other")
                by_sec[sec] += vol
                tot += vol
        return {s: (v / tot * 100 if tot else 0) for s, v in by_sec.items()}
    this_week = dates[-5:] if len(dates) >= 5 else dates
    last_week = dates[-10:-5] if len(dates) >= 10 else []
    tw, lw = week_share(this_week), week_share(last_week)
    rotation = []
    for sec in set(tw) | set(lw):
        a, b = tw.get(sec, 0), lw.get(sec, 0)
        rotation.append({
            "sector": sec,
            "thisWeekPct": round(a, 1), "lastWeekPct": round(b, 1),
            "deltaPct": round(a - b, 1),
        })
    rotation.sort(key=lambda r: -r["deltaPct"])

    # --- 5. flow echo: unusual today, was unusual 5-30d ago, quiet the last 4d ---
    echo = []
    mid = dates[-30:-4] if len(dates) > 5 else []
    recent = dates[-4:-1] if len(dates) > 1 else []
    for ticker in hist.get(today, {}):
        x_today, mult = is_unusual(ticker, today)
        if not x_today:
            continue
        if any(is_unusual(ticker, d)[0] for d in recent):
            continue  # that's a streak, not an echo
        last_seen = None
        for i, d in enumerate(reversed(mid)):
            if is_unusual(ticker, d)[0]:
                last_seen = i + 5  # days ago (5 = start of mid window offset)
                break
        if last_seen is None:
            continue
        side = side_of.get(ticker, "bull" if (px_chg.get(ticker) or 0) >= 0 else "bear")
        echo.append({"ticker": ticker, "side": side, "x": round(mult, 2),
                     "lastSeenDaysAgo": last_seen})
    echo.sort(key=lambda r: -r["x"])

    out = {
        "asOf": datetime.now(timezone.utc).isoformat(),
        "marketDate": market_date,
        "streaks": streaks,
        "confirmed": confirmed,
        "firstAppearance": first,
        "sectorRotation": rotation,
        "echo": echo,
    }
    with open(out_path, "w") as f:
        json.dump(out, f)
    print(f"wrote {out_path}: {len(streaks)} streaks, {len(confirmed)} confirmed, {len(first)} first-appearance")

if __name__ == "__main__":
    main()
