"""
update_prices.py
- data/prices/*.json    : {"date": "YYYY-MM-DD", "close": float}
- data/fx/USDKRW.json  : {"date": "YYYY-MM-DD", "rate": float}
- data/fear_greed.json : {"date": "YYYY-MM-DD", "score": float, "rating": str}
"""

import json, time, requests
from pathlib import Path
from datetime import datetime, timedelta
import yfinance as yf

PRICE_SYMBOLS = ["SPY", "QQQ", "GLD", "HYG", "TLT"]
FX_SYMBOL     = "USDKRW=X"
PRICES_DIR    = Path("data/prices")
FX_DIR        = Path("data/fx")
FG_PATH       = Path("data/fear_greed.json")
CNN_FG_URL    = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"

def load_json(path):
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return []

def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  Saved {path}")

def last_date(records):
    return records[-1]["date"] if records else None

def fetch_new_rows(ticker_symbol, after_date, field):
    start = (datetime.strptime(after_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") if after_date else "1990-01-01"
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if start > today:
        print(f"  {ticker_symbol}: already up to date")
        return []
    print(f"  {ticker_symbol}: fetching {start} ~ {today}...")
    df = yf.Ticker(ticker_symbol).history(start=start, end=today, auto_adjust=True)
    if df.empty:
        print(f"  {ticker_symbol}: no new data")
        return []
    rows = [{"date": dt.strftime("%Y-%m-%d"), field: round(float(row["Close"]), 4)} for dt, row in df.iterrows()]
    print(f"  {ticker_symbol}: +{len(rows)} rows")
    return rows

def score_to_rating(score):
    if score <= 25: return "Extreme Fear"
    if score <= 45: return "Fear"
    if score <= 55: return "Neutral"
    if score <= 75: return "Greed"
    return "Extreme Greed"

def update_fear_greed():
    existing = load_json(FG_PATH)
    if not isinstance(existing, list):
        existing = []
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if existing and existing[-1]["date"] == today:
        print(f"  Fear & Greed: already up to date")
        return
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://edition.cnn.com/markets/fear-and-greed"}
    try:
        res = requests.get(CNN_FG_URL, headers=headers, timeout=10)
        res.raise_for_status()
        fg = res.json()["fear_and_greed"]
        score = round(float(fg["score"]), 1)
        rating = fg.get("rating", score_to_rating(score))
        entry = {"date": today, "score": score, "rating": rating}
        existing.append(entry)
        save_json(FG_PATH, existing)
        print(f"  Fear & Greed: {score} ({rating})")
    except Exception as e:
        print(f"  Fear & Greed fetch failed: {e}")

def main():
    print(f"Update Start: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    for symbol in PRICE_SYMBOLS:
        path = PRICES_DIR / f"{symbol}.json"
        existing = load_json(path)
        new_rows = fetch_new_rows(symbol, last_date(existing), field="close")
        if new_rows:
            save_json(path, existing + new_rows)
        time.sleep(1)
    print("\n[FX]")
    fx_path = FX_DIR / "USDKRW.json"
    existing_fx = load_json(fx_path)
    new_fx = fetch_new_rows(FX_SYMBOL, last_date(existing_fx), field="rate")
    if new_fx:
        save_json(fx_path, existing_fx + new_fx)
    print("\n[Fear & Greed]")
    update_fear_greed()
    print("\nDone!")

if __name__ == "__main__":
    main()