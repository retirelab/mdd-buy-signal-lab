"""Keep a long CNN Fear & Greed history dated by US trading day.

- data/history/fear_greed_daily.json : {"seriesId","dateBasis","sources","updatedAt","observations":[{"date","score","rating","source"}]}
- data/history/fear_greed_daily-status.json

Why a separate file?  data/fear_greed.json (used by the dashboard's buy condition) stores the
score under the *collection* date (Korean morning ≈ previous US close).  This history file is dated
by the US trading day the score belongs to, so it can be joined with prices/*.json for backtests.

Sources (field "source" on every row):
- "archive": community archive github.com/whit3rabbit/fear-greed-data (2011-01-03 ~), rebuilt from
             CNN pages / Wayback Machine.  Not official; one known gap 2020-06-05 ~ 2020-07-09.
- "cnn"    : CNN graphdata endpoint "fear_and_greed_historical" (about the last year).  Wins on overlap.
On failure the existing file is kept; only the status file changes.
"""
import csv, io, json, math, os, sys, urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / 'data' / 'history'
CNN_URL = 'https://production.dataviz.cnn.io/index/fearandgreed/graphdata'
ARCHIVE_URL = 'https://raw.githubusercontent.com/whit3rabbit/fear-greed-data/main/fear-greed.csv'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://edition.cnn.com/markets/fear-and-greed',
    'Origin': 'https://edition.cnn.com',
}


def score_to_rating(score):  # same thresholds as scripts/update_prices.py
    if score <= 25: return 'Extreme Fear'
    if score <= 45: return 'Fear'
    if score <= 55: return 'Neutral'
    if score <= 75: return 'Greed'
    return 'Extreme Greed'


def row(day, score, source):
    if not math.isfinite(score) or not 0 <= score <= 100:
        raise ValueError(f'Out-of-range score on {day}')
    score = round(float(score), 1)
    return {'date': day, 'score': score, 'rating': score_to_rating(score), 'source': source}


def parse_archive(text):
    reader = csv.DictReader(io.StringIO(text.lstrip('﻿')))
    if 'Date' not in (reader.fieldnames or []) or 'Fear Greed' not in reader.fieldnames:
        raise ValueError('Unexpected archive columns')
    rows = [row(r['Date'], float(r['Fear Greed']), 'archive') for r in reader if r['Fear Greed'].strip()]
    if len(rows) < 2500:
        raise ValueError('Archive too short')
    return rows


def parse_cnn(payload):
    data = payload['fear_and_greed_historical']['data']
    rows = []
    for point in data:
        day = datetime.fromtimestamp(point['x'] / 1000, tz=timezone.utc).date().isoformat()
        rows.append(row(day, float(point['y']), 'cnn'))
    if len(rows) < 30:
        raise ValueError('CNN history too short')
    return rows


def merge(old_rows, *batches):
    """Later batches win on the same date; stored dates are never dropped."""
    by_date = {r['date']: r for r in old_rows}
    for batch in batches:
        for r in batch:
            by_date[r['date']] = r
    return sorted(by_date.values(), key=lambda r: r['date'])


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=1) + '\n')
    os.replace(temp, path)


def fetch(url, headers):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as res:
        return res.read(10_000_000).decode('utf-8-sig')


def update(root=ROOT, get_archive=None, get_cnn=None, now=None, use_archive=False):
    stamp = now or datetime.now(timezone.utc).isoformat(timespec='seconds')
    data_path, status_path = root / 'fear_greed_daily.json', root / 'fear_greed_daily-status.json'
    old = json.loads(data_path.read_text()) if data_path.exists() else None
    old_rows = (old or {}).get('observations', [])
    errors, batches = [], []
    if use_archive or not old_rows:
        try:
            batches.append(parse_archive(get_archive() if get_archive else fetch(ARCHIVE_URL, {'User-Agent': 'RetireLab-MDD/1.0'})))
        except Exception as exc:
            errors.append(f'archive: {type(exc).__name__}')
    try:
        payload = json.loads(get_cnn() if get_cnn else fetch(CNN_URL, HEADERS))
        batches.append(parse_cnn(payload))
    except Exception as exc:
        errors.append(f'cnn: {type(exc).__name__}')
    if not batches:
        write_json(status_path, {'checkedAt': stamp, 'ok': False,
                                 'latestDate': old_rows[-1]['date'] if old_rows else None,
                                 'error': '수집 실패 · 마지막 정상 자료 유지 (' + ', '.join(errors) + ')'})
        print('::warning::Fear & Greed history fetch failed; retained existing history', errors)
        return False
    rows = merge(old_rows, *batches)
    if rows != old_rows:
        write_json(data_path, {
            'seriesId': 'CNN_FEAR_GREED',
            'dateBasis': 'US trading date (score at that day\'s close)',
            'sources': {
                'archive': 'github.com/whit3rabbit/fear-greed-data (community archive, unofficial; gap 2020-06-05~2020-07-09)',
                'cnn': CNN_URL + ' (fear_and_greed_historical, ~1 year)'},
            'updatedAt': stamp, 'observations': rows})
    write_json(status_path, {'checkedAt': stamp, 'ok': not errors, 'latestDate': rows[-1]['date'],
                             'error': None if not errors else '일부 출처 실패: ' + ', '.join(errors)})
    print(f"Fear & Greed history: {len(rows)} days, {rows[0]['date']} ~ {rows[-1]['date']}")
    return True


if __name__ == '__main__':
    ok = update(use_archive='--archive' in sys.argv)
    sys.exit(0 if ok or (ROOT / 'fear_greed_daily.json').exists() else 1)
