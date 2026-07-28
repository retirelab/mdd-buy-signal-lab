"""
update_prices.py
- data/prices/*.json    : {"date": "YYYY-MM-DD", "close": float}
- data/fx/USDKRW.json  : {"date": "YYYY-MM-DD", "rate": float}
- data/fear_greed.json : {"date": "YYYY-MM-DD", "score": float, "rating": str}
- data/meta.json       : {"lastUpdate": "...", "symbols": {...}}
"""

import json, time, requests
from pathlib import Path
from datetime import datetime, timedelta, timezone
import yfinance as yf

PRICE_SYMBOLS = ["SPY", "QQQ", "GLD", "HYG", "TLT"]
FX_SYMBOL     = "USDKRW=X"
PRICES_DIR    = Path("data/prices")
FX_DIR        = Path("data/fx")
FG_PATH       = Path("data/fear_greed.json")
META_PATH     = Path("data/meta.json")
CNN_FG_URL    = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"

# 마지막 저장일로부터 며칠을 겹쳐서 다시 조회할지.
# 임시값/누락분을 자동 교정하는 안전장치. dedupe_by_date가 덮어쓰므로 중복 위험 없음.
OVERLAP_DAYS = 5


def utc_now():
    return datetime.now(timezone.utc)


def load_json(path):
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            print(f"  [warn] {path.name}: JSON 파싱 실패 ({e}). 빈 목록으로 시작합니다.")
            return []
    return []


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  Saved {path}")


def dedupe_by_date(records):
    """
    날짜 기준 중복 제거. 중복 시 뒤에 오는 값(= 새로 받은 값)이 우선.
    결과는 날짜 오름차순 정렬.
    """
    by_date = {}
    for row in records:
        if isinstance(row, dict) and "date" in row:
            by_date[row["date"]] = row
    return sorted(by_date.values(), key=lambda r: r["date"])


def load_and_clean(path):
    """읽으면서 중복 제거. 중복이 있었으면 파일도 정리해서 다시 저장."""
    raw = load_json(path)
    cleaned = dedupe_by_date(raw)
    if len(cleaned) != len(raw):
        print(f"  [cleanup] {path.name}: 중복 {len(raw) - len(cleaned)}건 제거")
        save_json(path, cleaned)
    return cleaned


def last_date(records):
    return records[-1]["date"] if records else None


def fetch_new_rows(ticker_symbol, after_date, field):
    """
    after_date 이후 데이터를 조회.
    - OVERLAP_DAYS 만큼 뒤로 물러서서 시작 (임시값 교정용)
    - end는 '내일'로 지정 (yfinance의 end는 exclusive라서 오늘을 포함시키려면 +1 필요)
    """
    if after_date:
        base = datetime.strptime(after_date, "%Y-%m-%d")
        start_dt = base - timedelta(days=OVERLAP_DAYS)
        start = start_dt.strftime("%Y-%m-%d")
    else:
        start = "1990-01-01"

    # end는 exclusive. 내일로 줘야 오늘 종가까지 포함된다.
    tomorrow = (utc_now() + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"  {ticker_symbol}: fetching {start} ~ {tomorrow} (end exclusive)...")
    try:
        df = yf.Ticker(ticker_symbol).history(start=start, end=tomorrow, auto_adjust=True)
    except Exception as e:
        print(f"  {ticker_symbol}: fetch 실패 - {e}")
        return []

    if df.empty:
        print(f"  {ticker_symbol}: 새 데이터 없음")
        return []

    rows = []
    for dt, row in df.iterrows():
        close = row["Close"]
        if close is None:
            continue
        try:
            val = float(close)
        except (TypeError, ValueError):
            continue
        if val != val or val <= 0:   # NaN 또는 비정상값 제외
            continue
        rows.append({"date": dt.strftime("%Y-%m-%d"), field: round(val, 4)})

    if rows:
        print(f"  {ticker_symbol}: {len(rows)}행 수신 (마지막: {rows[-1]['date']})")
    else:
        print(f"  {ticker_symbol}: 유효한 행 없음")
    return rows


def score_to_rating(score):
    if score <= 25: return "Extreme Fear"
    if score <= 45: return "Fear"
    if score <= 55: return "Neutral"
    if score <= 75: return "Greed"
    return "Extreme Greed"


def update_fear_greed():
    existing = load_and_clean(FG_PATH)
    today = utc_now().strftime("%Y-%m-%d")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://edition.cnn.com/markets/fear-and-greed",
        "Origin": "https://edition.cnn.com"
    }
    try:
        res = requests.get(CNN_FG_URL, headers=headers, timeout=10)
        res.raise_for_status()
        fg = res.json()["fear_and_greed"]
        score = round(float(fg["score"]), 1)
        rating = fg.get("rating", score_to_rating(score)).title()
        entry = {"date": today, "score": score, "rating": rating}
        # 같은 날짜면 덮어쓰기 (하루 여러 번 실행해도 안전)
        merged = dedupe_by_date(existing + [entry])
        save_json(FG_PATH, merged)
        print(f"  Fear & Greed: {score} ({rating})")
    except Exception as e:
        print(f"  Fear & Greed 조회 실패: {e}")


def write_meta(price_last_dates, fx_last_date):
    """앱에서 최신 여부를 가볍게 확인할 수 있는 메타 파일."""
    meta = {
        "lastUpdate": utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "prices": price_last_dates,
        "fx": fx_last_date
    }
    save_json(META_PATH, meta)


def main():
    print(f"Update Start: {utc_now().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"(overlap={OVERLAP_DAYS}일 재조회, end=exclusive 보정 적용)\n")

    price_last_dates = {}

    print("[Prices]")
    for symbol in PRICE_SYMBOLS:
        path = PRICES_DIR / f"{symbol}.json"
        existing = load_and_clean(path)
        new_rows = fetch_new_rows(symbol, last_date(existing), field="close")
        merged = dedupe_by_date(existing + new_rows)
        if merged and (len(merged) != len(existing) or merged != existing):
            save_json(path, merged)
        price_last_dates[symbol] = last_date(merged)
        time.sleep(1)

    print("\n[FX]")
    fx_path = FX_DIR / "USDKRW.json"
    existing_fx = load_and_clean(fx_path)
    new_fx = fetch_new_rows(FX_SYMBOL, last_date(existing_fx), field="rate")
    merged_fx = dedupe_by_date(existing_fx + new_fx)
    if merged_fx and (len(merged_fx) != len(existing_fx) or merged_fx != existing_fx):
        save_json(fx_path, merged_fx)
    fx_last = last_date(merged_fx)

    print("\n[Fear & Greed]")
    update_fear_greed()

    print("\n[Meta]")
    write_meta(price_last_dates, fx_last)

    print("\nDone!")
    print("최종 상태:")
    for sym, d in price_last_dates.items():
        print(f"  {sym}: {d}")
    print(f"  USDKRW: {fx_last}")


if __name__ == "__main__":
    main()