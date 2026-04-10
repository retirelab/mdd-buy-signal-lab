"""
update_prices.py
- data/prices/*.json  : {"date": "YYYY-MM-DD", "close": float}
- data/fx/USDKRW.json : {"date": "YYYY-MM-DD", "close": float}
기존 데이터 마지막 날짜 이후 새 데이터만 append 함
"""

import json, os, time
from pathlib import Path
from datetime import datetime, timedelta
import yfinance as yf

# ─── 설정 ─────────────────────────────────────────────
PRICE_SYMBOLS = ["SPY", "QQQ", "GLD", "HYG", "TLT"]
FX_SYMBOL     = "USDKRW=X"

PRICES_DIR = Path("data/prices")
FX_DIR     = Path("data/fx")
# ───────────────────────────────────────────────────────


def load_json(path: Path) -> list:
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return []


def save_json(path: Path, data: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  ✅ Saved {path} ({len(data)} rows)")


def last_date(records: list) -> str | None:
    """기존 레코드의 마지막 날짜 반환"""
    if not records:
        return None
    return records[-1]["date"]


def fetch_new_rows(ticker_symbol: str, after_date: str | None, field: str = "close") -> list:
    """
    after_date 이후 날짜의 신규 데이터만 반환
    field: JSON에 저장할 키 이름 ("close")
    """
    start = (
        (datetime.strptime(after_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        if after_date
        else "1990-01-01"
    )
    today = datetime.utcnow().strftime("%Y-%m-%d")

    if start > today:
        print(f"  ℹ️  {ticker_symbol}: 이미 최신 ({after_date})")
        return []

    print(f"  📥 {ticker_symbol}: {start} ~ {today} 다운로드 중...")
    ticker = yf.Ticker(ticker_symbol)
    df = ticker.history(start=start, end=today, auto_adjust=True)

    if df.empty:
        print(f"  ⚠️  {ticker_symbol}: 새 데이터 없음")
        return []

    rows = []
    for dt, row in df.iterrows():
        date_str = dt.strftime("%Y-%m-%d")
        close_val = round(float(row["Close"]), 4)
        rows.append({"date": date_str, field: close_val})

    print(f"  ✅ {ticker_symbol}: {len(rows)}개 신규 행 추가")
    return rows


# ─── 메인 ─────────────────────────────────────────────
def main():
    print("=" * 50)
    print(f"🔄 Price Update Start: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 50)

    # 1) 가격 데이터 업데이트
    for symbol in PRICE_SYMBOLS:
        path = PRICES_DIR / f"{symbol}.json"
        existing = load_json(path)
        new_rows = fetch_new_rows(symbol, last_date(existing), field="close")

        if new_rows:
            updated = existing + new_rows
            save_json(path, updated)

        time.sleep(1)   # Yahoo Finance rate limit 방지

    # 2) 환율 데이터 업데이트
    print("\n[FX]")
    fx_path = FX_DIR / "USDKRW.json"
    existing_fx = load_json(fx_path)
    new_fx_rows = fetch_new_rows(FX_SYMBOL, last_date(existing_fx), field="close")

    if new_fx_rows:
        updated_fx = existing_fx + new_fx_rows
        save_json(fx_path, updated_fx)

    print("\n✨ 업데이트 완료!")


if __name__ == "__main__":
    main()
