"""Keyless KODEX 200 chart history. Keep original provider basis and closed sessions only."""
import ast,json,math,os,sys,urllib.request
from datetime import datetime,time,timezone
from zoneinfo import ZoneInfo
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
SOURCE='https://api.finance.naver.com/siseJson.naver?symbol=069500&requestType=1&startTime=20021014&endTime={end}&timeframe=day'
def parse(text,now=None):
 now=now or datetime.now(ZoneInfo('Asia/Seoul'))
 if now.tzinfo is None:raise ValueError('Timezone required')
 now=now.astimezone(ZoneInfo('Asia/Seoul'))
 raw=ast.literal_eval(text.strip());rows=[];seen=set()
 if not raw or raw[0][0]!='날짜' or raw[0][4]!='종가':raise ValueError('Wrong source columns')
 for r in raw[1:]:
  day=datetime.strptime(str(r[0]),'%Y%m%d').date();value=float(r[4])
  if day>now.date() or (day==now.date() and now.time()<time(18)):continue
  if not math.isfinite(value) or value<=0 or day in seen:raise ValueError('Invalid/duplicate price')
  seen.add(day);rows.append({'date':day.isoformat(),'close':value})
 rows.sort(key=lambda r:r['date'])
 if len(rows)<200 or rows[0]['date']!='2002-10-14':raise ValueError('Truncated history')
 return rows

def write(path,value):
 path.parent.mkdir(parents=True,exist_ok=True);temp=path.with_suffix('.tmp');temp.write_text(json.dumps(value,ensure_ascii=False,separators=(',',':'))+'\n');os.replace(temp,path)

def update(root=ROOT,text=None,now=None):
 now=now or datetime.now(ZoneInfo('Asia/Seoul'));path=root/'data/prices/069500.json';meta=root/'data/prices/069500-meta.json'
 old=json.loads(path.read_text()) if path.exists() else []
 try:
  url=SOURCE.format(end=now.strftime('%Y%m%d'))
  if text is None:
   req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0','Referer':'https://finance.naver.com/'})
   with urllib.request.urlopen(req,timeout=40) as response:text=response.read(3_000_000).decode('utf-8-sig')
  rows=parse(text,now)
  if old and not {r['date'] for r in old}.issubset({r['date'] for r in rows}):raise ValueError('Missing saved dates')
  if old!=rows:write(path,rows)
  write(meta,{'symbol':'069500','name':'KODEX 200','currency':'KRW','source':'NAVER Finance chart','sourceUrl':url,'priceBasis':'provider-chart','basisNote':'제공처 차트 가격 기준 · 과거 가격은 사후 조정될 수 있음 · 분배금 재투자 수익률 아님','firstDate':rows[0]['date'],'lastDate':rows[-1]['date'],'count':len(rows),'checkedAt':now.isoformat(),'ok':True})
  print(f'KODEX 200: {len(rows)} sessions {rows[0]["date"]} to {rows[-1]["date"]}')
  return True
 except Exception as e:
  status=json.loads(meta.read_text()) if meta.exists() else {'symbol':'069500'}
  status.update({'checkedAt':now.isoformat(),'ok':False,'error':'가격 수집 실패 · 마지막 정상 자료 유지'})
  write(meta,status);print(f'::warning::KODEX 200 collection failed ({type(e).__name__}); preserved history');return False
if __name__=='__main__':
 ok=update();sys.exit(0 if ok or (ROOT/'data/prices/069500.json').exists() else 1)
