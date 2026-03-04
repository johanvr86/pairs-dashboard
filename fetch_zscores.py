"""
fetch_zscores.py  -  Fetch all pair z-scores and save to data/zscores.json
Run daily (or intraday) via GitHub Actions. Outputs JSON consumed by dashboard.
"""
import warnings, json, os
warnings.filterwarnings("ignore")
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, date

LOOKBACK = 60

PAIRS = {
    # Active bots
    "Brent/WTI":     ("BZ=F",      "CL=F",      "Oil pairs",        "bot"),
    "Nasdaq/Gold":   ("NQ=F",      "GC=F",       "AI bubble",        "bot"),
    "SEMI/ITWN":     ("SOXX",      "EWT",        "Taiwan tension",   "bot"),
    # Silver standalone
    "Silver":        ("SI=F",      None,          "MACD momentum",   "bot"),
    # Geopolitical
    "Wheat/Soybeans":("ZW=F",      "ZS=F",       "Ukraine",          "watch"),
    "SOXX/TSM":      ("SOXX",      "TSM",        "Taiwan",           "watch"),
    "Gold/TLT":      ("GC=F",      "TLT",        "Safe haven",       "watch"),
    "Oil/Airlines":  ("CL=F",      "JETS",       "Iran/Hormuz",      "watch"),
    "XLY/XLP":       ("XLY",       "XLP",        "Tariffs",          "watch"),
    "Copper/Gold":   ("HG=F",      "GC=F",       "Growth signal",    "watch"),
    "Israel/Egypt":  ("EIS",       "EGPT",       "Middle East",      "watch"),
    "Steel/Gold":    ("SLX",       "GC=F",       "Rearmament",       "watch"),
    "DX/Gold":       ("DX-Y.NYB",  "GC=F",       "Dollar confidence","watch"),
    "Wheat/Corn":    ("ZW=F",      "ZC=F",       "Ukraine grains",   "watch"),
}

ENTRY_Z = 2.0

def fetch_history(ticker, period="6mo"):
    try:
        df = yf.download(ticker, period=period, interval="1d",
                         auto_adjust=True, progress=False)["Close"]
        if isinstance(df, pd.DataFrame): df = df.iloc[:,0]
        return df.dropna()
    except:
        return None

def zscore_series(a, b=None):
    if b is None:
        ratio = a
    else:
        ratio = a / b
    mean  = ratio.rolling(LOOKBACK).mean()
    std   = ratio.rolling(LOOKBACK).std()
    z     = (ratio - mean) / std
    return z, ratio, mean

def run():
    os.makedirs("data", exist_ok=True)
    results = {}
    print(f"Fetching {len(PAIRS)} pairs...")

    for name, (ta, tb, theme, category) in PAIRS.items():
        try:
            a = fetch_history(ta)
            if a is None or len(a) < LOOKBACK + 10:
                print(f"  x {name} — no data for {ta}")
                continue

            if tb:
                b = fetch_history(tb)
                if b is None or len(b) < LOOKBACK + 10:
                    print(f"  x {name} — no data for {tb}")
                    continue
                combined = pd.DataFrame({"a": a, "b": b}).dropna()
                a_aligned = combined["a"]
                b_aligned = combined["b"]
            else:
                a_aligned = a
                b_aligned = None

            z_series, ratio_series, mean_series = zscore_series(a_aligned, b_aligned)
            z_series    = z_series.dropna()
            ratio_series = ratio_series.loc[z_series.index]
            mean_series  = mean_series.loc[z_series.index]

            cur_z    = round(float(z_series.iloc[-1]), 3)
            prev_z   = round(float(z_series.iloc[-2]), 3)
            cur_r    = round(float(ratio_series.iloc[-1]), 4)
            cur_mean = round(float(mean_series.iloc[-1]), 4)

            # Signal state
            if abs(cur_z) >= ENTRY_Z and abs(prev_z) < ENTRY_Z:
                signal = "entry"
            elif abs(cur_z) >= ENTRY_Z:
                signal = "active"
            elif abs(cur_z) >= 1.5:
                signal = "watch"
            else:
                signal = "normal"

            # History for chart (dates + z-scores)
            hist_z = z_series.tail(130)   # ~6 months trading days
            hist_r = ratio_series.loc[hist_z.index]

            results[name] = {
                "name":       name,
                "ticker_a":   ta,
                "ticker_b":   tb or "",
                "theme":      theme,
                "category":   category,
                "cur_z":      cur_z,
                "prev_z":     prev_z,
                "cur_ratio":  cur_r,
                "mean_ratio": cur_mean,
                "signal":     signal,
                "pct_to_entry": round(abs(cur_z) / ENTRY_Z * 100, 1),
                "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
                "history": {
                    "dates":    [str(d.date()) for d in hist_z.index],
                    "zscores":  [round(float(v), 3) for v in hist_z.values],
                    "ratios":   [round(float(v), 4) for v in hist_r.values],
                    "prices_a": [round(float(v), 4) for v in a_aligned.loc[hist_z.index].values],
                    "prices_b": [round(float(v), 4) for v in b_aligned.loc[hist_z.index].values] if b_aligned is not None else [],
                    "timestamps": [datetime.utcnow().strftime("%H:%M UTC") if str(d.date()) == str(date.today()) else "close" for d in hist_z.index],
                }
            }
            print(f"  + {name:<20} z={cur_z:+.2f}  [{signal}]")

        except Exception as e:
            print(f"  x {name} — error: {e}")

    # Write JSON
    output = {
        "updated":    datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "entry_z":    ENTRY_Z,
        "lookback":   LOOKBACK,
        "pairs":      results,
    }
    with open("data/zscores.json", "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved data/zscores.json  ({len(results)} pairs)")

if __name__ == "__main__":
    run()
