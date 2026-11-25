import importlib.util,json,unittest,tempfile
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
spec=importlib.util.spec_from_file_location('k',Path(__file__).resolve().parents[1]/'scripts/update_kodex.py');k=importlib.util.module_from_spec(spec);spec.loader.exec_module(k)
ROOT=Path(__file__).resolve().parents[1]
class Tests(unittest.TestCase):
 def fixture(self):
  rows=json.loads((ROOT/'data/prices/069500.json').read_text());return [['날짜','시가','고가','저가','종가','거래량']]+[[r['date'].replace('-',''),1,1,1,r['close'],1] for r in rows]
 def test_open_session_excluded_and_closed_session_retained(self):
  rows=self.fixture();day=rows[-1][0];before=datetime.strptime(day+' 17:59','%Y%m%d %H:%M').replace(tzinfo=ZoneInfo('Asia/Seoul'));after=before.replace(hour=18,minute=0)
  self.assertEqual(len(k.parse(repr(rows),before)),len(rows)-2);self.assertEqual(len(k.parse(repr(rows),after)),len(rows)-1)
 def test_corrupt_payload_preserves_saved_prices(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t);p=root/'data/prices/069500.json';p.parent.mkdir(parents=True);p.write_text(json.dumps(self.fixture()[1:]));before=p.read_bytes();self.assertFalse(k.update(root,text='<html>bad</html>'));self.assertEqual(before,p.read_bytes())
 def test_duplicates_and_truncated_history_rejected(self):
  r=self.fixture()
  for rows in [r[:30],r+[r[3]]]:
   with self.assertRaises(ValueError):k.parse(repr(rows),datetime(2026,11,25,19,tzinfo=ZoneInfo('Asia/Seoul')))
if __name__=='__main__':unittest.main()
