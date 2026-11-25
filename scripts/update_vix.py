"""Download the public FRED VIXCLS CSV (CBOE VIX close, daily since 1990) and keep history safe.

- data/macro/VIXCLS.json        : {"seriesId","sourceUrl","units","frequency","updatedAt","origin","observations":[{"date","value"}]}
- data/macro/VIXCLS-status.json : last fetch result (ok / error), like UNRATE-status.json

Rules (same spirit as update_unemployment.py):
- never shrink history: dates already stored must still exist after merge
- FRED values win on overlapping dates; older backfill rows that FRED lacks are kept
- on any failure the existing file is left untouched and only the status file changes
"""
import csv, io, json, math, os, sys, urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

URL = 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS'
ROOT = Path(__file__).resolve().parents[1] / 'data' / 'macro'
MIN_ROWS = 5000          # FRED VIXCLS has ~9,000 trading days since 1990
VALID = (5.0, 150.0)     # VIX close has stayed within ~9–83 historically


def parse_csv(text):
    reader = csv.DictReader(io.StringIO(text.lstrip('﻿')))
    fields = reader.fieldnames or []
    date_key = 'observation_date' if 'observation_date' in fields else 'DATE'
    if date_key not in fields or 'VIXCLS' not in fields:
        raise ValueError('Unexpected CSV columns')
    rows, seen = [], set()
    for row in reader:
        day = date.fromisoformat(row[date_key])
        raw = row['VIXCLS'].strip()
        value = None if raw in ('', '.') else float(raw)
        if day > date.today() or day in seen:
            raise ValueError('Future or duplicate observation')
        if value is not None and (not math.isfinite(value) or not VALID[0] <= value <= VALID[1]):
            raise ValueError(f'Out-of-range VIX value on {day}')
        seen.add(day)
        rows.append({'date': day.isoformat(), 'value': value})
    rows.sort(key=lambda r: r['date'])
    if len(rows) < MIN_ROWS:
        raise ValueError('Source history is too short')
    if all(r['value'] is None for r in rows[-5:]):
        raise ValueError('Latest values are missing')
    return rows


def merge(old, incoming):
    """FRED wins on overlap; keep stored rows FRED does not return (e.g. backfill)."""
    by_date = {r['date']: r for r in (old or {}).get('observations', [])}
    incoming_dates = {r['date'] for r in incoming}
    stored_dates = set(by_date)
    # FRED should cover the stored FRED range; a big hole means a truncated response
    first_in = min(incoming_dates)
    missing = [d for d in stored_dates if d >= first_in and d not in incoming_dates]
    if len(missing) > 30:
        raise ValueError('Source history is truncated')
    for r in incoming:
        if r['value'] is None and by_date.get(r['date'], {}).get('value') is not None:
            continue  # do not blank out a value we already have
        by_date[r['date']] = {'date': r['date'], 'value': r['value']}
    return sorted(by_date.values(), key=lambda r: r['date'])


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    os.replace(temp, path)


def update(root=ROOT, download=None, now=None):
    stamp = now or datetime.now(timezone.utc).isoformat(timespec='seconds')
    data_path, status_path = root / 'VIXCLS.json', root / 'VIXCLS-status.json'
    old = json.loads(data_path.read_text()) if data_path.exists() else None
    try:
        if download is None:
            req = urllib.request.Request(URL, headers={'User-Agent': 'RetireLab-MDD/1.0', 'Accept': 'text/csv'})
            with urllib.request.urlopen(req, timeout=30) as response:
                text = response.read(5_000_000).decode('utf-8-sig')
        else:
            text = download()
        rows = merge(old, parse_csv(text))
        if not old or old.get('observations') != rows:
            write_json(data_path, {
                'seriesId': 'VIXCLS', 'sourceUrl': URL, 'units': 'Index', 'frequency': 'Daily (close)',
                'updatedAt': stamp, 'origin': 'FRED CSV (CBOE VIX close); pre-FRED rows from CBOE backfill',
                'observations': rows})
        latest = next(r for r in reversed(rows) if r['value'] is not None)
        write_json(status_path, {'checkedAt': stamp, 'ok': True, 'latestDate': latest['date'], 'error': None})
        print(f"VIXCLS: {len(rows)} days, latest {latest['date']} = {latest['value']}")
        return True
    except Exception as exc:
        last = None
        if old:
            vals = [r for r in old.get('observations', []) if r.get('value') is not None]
            last = vals[-1]['date'] if vals else None
        write_json(status_path, {'checkedAt': stamp, 'ok': False, 'latestDate': last,
                                 'error': 'CSV 수집 실패 · 마지막 정상 자료 유지'})
        print(f'::warning::VIXCLS fetch failed ({type(exc).__name__}: {exc}); retained existing history')
        return False


if __name__ == '__main__':
    ok = update()
    sys.exit(0 if ok or (ROOT / 'VIXCLS.json').exists() else 1)
