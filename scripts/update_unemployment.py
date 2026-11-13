"""Download public FRED CSV; preserve valid history if the source fails."""
import csv, io, json, math, os, sys, urllib.request
from datetime import date, datetime, timezone
from pathlib import Path
URL='https://fred.stlouisfed.org/graph/fredgraph.csv?id=UNRATE'
ROOT=Path(__file__).resolve().parents[1]/'data'/'macro'

def parse_csv(text):
    reader=csv.DictReader(io.StringIO(text.lstrip('\ufeff')))
    date_key='observation_date' if 'observation_date' in (reader.fieldnames or []) else 'DATE'
    if date_key not in (reader.fieldnames or []) or 'UNRATE' not in reader.fieldnames:
        raise ValueError('Unexpected CSV columns')
    rows=[];seen=set()
    for row in reader:
        day=date.fromisoformat(row[date_key]);value=None if row['UNRATE'].strip() in ('','.') else float(row['UNRATE'])
        if day.day!=1 or day>date.today() or (value is not None and (not math.isfinite(value) or not 0<=value<=100)) or day in seen:
            raise ValueError('Invalid or duplicate monthly observation')
        seen.add(day);rows.append({'date':day.isoformat(),'value':value})
    rows.sort(key=lambda r:r['date'])
    if not rows or rows[-1]['value'] is None:raise ValueError('Latest value is missing')
    if len(rows)<12:raise ValueError('Less than 12 months returned')
    for a,b in zip(rows,rows[1:]):
        da=date.fromisoformat(a['date']);db=date.fromisoformat(b['date'])
        if db.year*12+db.month-da.year*12-da.month!=1:raise ValueError('Missing calendar month')
    return rows

def merge(old,incoming):
    before={r['date'] for r in (old or {}).get('observations',[])}
    if not before.issubset({r['date'] for r in incoming}):raise ValueError('Source history is truncated')
    return incoming

def write_json(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix('.tmp');temp.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n');os.replace(temp,path)

def update(root=ROOT,download=None,now=None):
    stamp=now or datetime.now(timezone.utc).isoformat(timespec='seconds')
    data_path=root/'UNRATE.json';status_path=root/'UNRATE-status.json'
    old=json.loads(data_path.read_text()) if data_path.exists() else None
    try:
        if download is None:
            req=urllib.request.Request(URL,headers={'User-Agent':'RetireLab-MDD/1.0','Accept':'text/csv'})
            with urllib.request.urlopen(req,timeout=30) as response:text=response.read(2_000_000).decode('utf-8-sig')
        else:text=download()
        rows=merge(old,parse_csv(text))
        if not old or old.get('observations')!=rows or old.get('origin')!='FRED CSV':
            write_json(data_path,{'seriesId':'UNRATE','sourceUrl':URL,'units':'Percent','frequency':'Monthly','seasonallyAdjusted':True,'updatedAt':stamp,'origin':'FRED CSV','observations':rows})
        write_json(status_path,{'checkedAt':stamp,'ok':True,'latestMonth':rows[-1]['date'],'error':None})
        print(f'UNRATE: {len(rows)} months, latest {rows[-1]["date"]}')
        return True
    except Exception as exc:
        write_json(status_path,{'checkedAt':stamp,'ok':False,'latestMonth':old['observations'][-1]['date'] if old else None,'error':'CSV 수집 실패 · 마지막 정상 자료 유지'})
        print(f'::warning::UNRATE fetch failed ({type(exc).__name__}); retained existing history')
        return False

if __name__=='__main__':
    ok=update()
    sys.exit(0 if ok or (ROOT/'UNRATE.json').exists() else 1)
