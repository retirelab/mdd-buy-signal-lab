import importlib.util,json,tempfile,unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('v',Path(__file__).resolve().parents[1]/'scripts/update_vix.py');v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
from datetime import date,timedelta
def csv_text(n=6000,start=date(2000,1,3),blank=None):
 out=['observation_date,VIXCLS'];d=start
 for i in range(n):
  out.append(f"{d.isoformat()},{'.' if blank==i else f'{15+i%10:.2f}'}");d+=timedelta(days=1)
 return '\n'.join(out)+'\n'
class VixTests(unittest.TestCase):
 def test_rejects_html_short_and_out_of_range(self):
  for text in ['<html>blocked</html>',csv_text(100),csv_text().replace('15.00','999.00')]:
   with self.assertRaises((ValueError,TypeError)):v.parse_csv(text)
 def test_missing_value_does_not_erase_stored_value(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t);self.assertTrue(v.update(root,lambda:csv_text(),'a'))
   self.assertTrue(v.update(root,lambda:csv_text(blank=10),'b'))
   obs=json.loads((root/'VIXCLS.json').read_text())['observations'];self.assertIsNotNone(obs[10]['value'])
 def test_failed_fetch_keeps_history_and_backfill_survives(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t);early=csv_text(6000,date(1990,1,2));self.assertTrue(v.update(root,lambda:early,'a'))
   later=csv_text(6000,date(1995,1,2));self.assertTrue(v.update(root,lambda:later,'b'))
   obs=json.loads((root/'VIXCLS.json').read_text())['observations'];self.assertEqual(obs[0]['date'],'1990-01-02')
   good=(root/'VIXCLS.json').read_bytes();self.assertFalse(v.update(root,lambda:'oops','c'))
   self.assertEqual(good,(root/'VIXCLS.json').read_bytes());self.assertFalse(json.loads((root/'VIXCLS-status.json').read_text())['ok'])
 def test_truncated_source_is_rejected(self):
  old={'observations':v.parse_csv(csv_text())}
  holed=[r for i,r in enumerate(old['observations']) if not 100<=i<200]
  with self.assertRaises(ValueError):v.merge(old,holed)
if __name__=='__main__':unittest.main()
