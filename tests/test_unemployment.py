import importlib.util,json,tempfile,unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('u',Path(__file__).resolve().parents[1]/'scripts/update_unemployment.py');u=importlib.util.module_from_spec(spec);spec.loader.exec_module(u)
CSV='observation_date,UNRATE\n'+''.join(f'2025-{m:02}-01,4.0\n' for m in range(1,13))
class CollectorTests(unittest.TestCase):
 def test_parser_rejects_html_duplicates_and_truncation(self):
  for text in ['<html>blocked</html>',CSV+'2025-01-01,4.0\n',CSV.replace('2025-03-01,4.0\n',''),CSV.replace('4.0','NaN')]:
   with self.assertRaises((ValueError,TypeError)):u.parse_csv(text)
 def test_missing_observation_is_preserved(self):
  rows=u.parse_csv(CSV.replace('2025-03-01,4.0','2025-03-01,.'));self.assertIsNone(rows[2]['value'])
 def test_repeat_revision_and_failed_fetch_are_safe(self):
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);self.assertTrue(u.update(root,lambda:CSV,'first'));before=(root/'UNRATE.json').read_bytes()
   self.assertTrue(u.update(root,lambda:CSV,'second'));self.assertEqual(before,(root/'UNRATE.json').read_bytes())
   revised=CSV.replace('2025-03-01,4.0','2025-03-01,4.2');self.assertTrue(u.update(root,lambda:revised,'third'))
   good=(root/'UNRATE.json').read_bytes();self.assertFalse(u.update(root,lambda:'oops','fourth'));self.assertEqual(good,(root/'UNRATE.json').read_bytes());self.assertFalse(json.loads((root/'UNRATE-status.json').read_text())['ok'])
 def test_history_cannot_disappear(self):
  old={'observations':u.parse_csv(CSV)}
  with self.assertRaises(ValueError):u.merge(old,old['observations'][1:])
if __name__=='__main__':unittest.main()
