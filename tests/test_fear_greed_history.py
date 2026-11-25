import importlib.util,json,tempfile,unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('f',Path(__file__).resolve().parents[1]/'scripts/update_fear_greed_history.py');f=importlib.util.module_from_spec(spec);spec.loader.exec_module(f)
from datetime import date,timedelta,datetime,timezone
def archive(n=3000):
 d=date(2011,1,3);lines=['Date,Fear Greed,Rating']
 for i in range(n):lines.append(f'{d.isoformat()},{20+i%60}.0,x');d+=timedelta(days=1)
 return '\n'.join(lines)+'\n'
def weekdays(n,start):
 out=[];d=start
 while len(out)<n:
  if d.weekday()<5:out.append(d)
  d+=timedelta(days=1)
 return out
def cnn(days=40,start=date(2019,3,1),score=10.0,extra=()):
 ds=weekdays(days,start)+list(extra)
 pts=[{'x':datetime(*d.timetuple()[:3],tzinfo=timezone.utc).timestamp()*1000,'y':score} for d in ds]
 return json.dumps({'fear_and_greed_historical':{'data':pts}})
def boom():raise ConnectionError('down')
class FearGreedHistoryTests(unittest.TestCase):
 def test_rejects_bad_inputs(self):
  for bad in ['<html></html>',archive(10),archive().replace('20.0','120.0')]:
   with self.assertRaises((ValueError,KeyError)):f.parse_archive(bad)
  with self.assertRaises((ValueError,KeyError)):f.parse_cnn(json.loads(cnn(5)))
 def test_cnn_wins_on_overlap_and_rating_matches_app(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t);self.assertTrue(f.update(root,lambda:archive(),lambda:cnn(),'a',use_archive=True))
   obs={r['date']:r for r in json.loads((root/'fear_greed_daily.json').read_text())['observations']}
   self.assertEqual(obs['2019-03-01']['source'],'cnn');self.assertEqual(obs['2019-03-01']['rating'],'Extreme Fear');self.assertEqual(obs['2011-01-03']['source'],'archive')
 def test_failure_keeps_file_and_history_never_shrinks(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t);f.update(root,lambda:archive(),boom,'a',use_archive=True);good=(root/'fear_greed_daily.json').read_bytes()
   self.assertFalse(f.update(root,boom,boom,'b'));self.assertEqual(good,(root/'fear_greed_daily.json').read_bytes())
   self.assertTrue(f.update(root,boom,lambda:cnn(40,date(2024,1,1)),'c'))
   n=len(json.loads((root/'fear_greed_daily.json').read_text())['observations']);self.assertEqual(n,3040)
 def test_drops_live_today_and_weekend_points(self):
  rows=f.parse_cnn(json.loads(cnn(40,date(2026,9,1),extra=[date(2026,11,21),date(2026,11,25)])),today=date(2026,11,25))
  days={r['date'] for r in rows};self.assertNotIn('2026-11-21',days);self.assertNotIn('2026-11-25',days);self.assertEqual(len(rows),40)
if __name__=='__main__':unittest.main()
