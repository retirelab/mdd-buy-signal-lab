// Run with: node --test tests/history.test.cjs
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
const context = vm.createContext({});
vm.runInContext(`
  const FIXED_DD_STEPS = [-5,-10,-15,-20,-25,-30,-35,-40,-45,-50,-55,-60,-65,-70,-75,-80,-85,-90,-95,-100];
  const isValidPositiveNumber = n => Number.isFinite(n) && n > 0;
  ${html.slice(html.indexOf('    function filterRawDataByDateRange'), html.indexOf('    const HISTORY_VERSION'))}
  this.api = { computeSignalStats, runEngine, calendarDaysBetween };
`, context);
const { computeSignalStats: stats, runEngine, calendarDaysBetween } = context.api;
function series(prices, dates) {
  let ath = 0;
  return prices.map((price, i) => {
    ath = Math.max(ath, price);
    return { price, ath, dd: price / ath - 1,
      date: dates ? dates[i] : new Date(Date.UTC(2020, 0, 1 + i)).toISOString().slice(0, 10) };
  });
}
test('recrossings before ATH recovery count as one episode', () => {
  const s = stats(series([100,84,86,84,100]), -.15);
  assert.equal(s.signalCount, 1); assert.equal(s.winCount, 1);
  assert.equal(s.pendingCount, 0); assert.equal(s.episodes[0].recoveryDays, 3);
});
test('new episode opens only after prior ATH has recovered', () => {
  const s = stats(series([100,84,100,110,90,110]), -.15);
  assert.equal(s.signalCount, 2); assert.equal(s.winCount, 2);
  assert.equal(s.episodes[1].entryAth, 110);
});
test('unrecovered episode stays pending, including entry on final day', () => {
  const s = stats(series([100,84,100,84]), -.15);
  assert.equal(s.pendingCount, 1); assert.equal(s.winCount, 1);
  assert.equal(s.episodes[1].recoveryDays, null);
  assert.equal(s.episodes[1].observedDays, 0);
});
test('breakeven and ATH recovery are separate; worst drop uses actual entry price', () => {
  const s = stats(series([100,80,60,80,99,100]), -.15);
  const e = s.episodes[0];
  assert.equal(e.additionalDd, -.25); assert.equal(e.breakevenDays, 2);
  assert.equal(e.recoveryDays, 4); assert.equal(e.lowDate, '2020-01-03');
});
test('low ends at ATH recovery, not at later unrelated crashes', () => {
  const s = stats(series([100,84,100,50]), -.15);
  assert.equal(s.episodes[0].lowPrice, 84); assert.equal(s.episodes[1].lowPrice, 50);
});
test('365-day rate excludes all recent entries, even early recoveries', () => {
  const dates = ['2020-01-01','2020-01-02','2020-01-03','2021-01-02','2021-01-03','2021-01-04'];
  const s = stats(series([100,84,100,100,84,100], dates), -.15);
  assert.equal(s.signalCount, 2); assert.equal(s.oneYearCount, 1);
  assert.equal(s.oneYearRecovered, 1); assert.equal(s.oneYearRate, 1);
});
test('exact 365 days counts as recovery within year; day 366 does not', () => {
  const a = stats(series([100,84,100], ['2019-12-31','2020-01-01','2020-12-31']), -.15);
  const b = stats(series([100,84,100], ['2019-12-31','2020-01-01','2021-01-01']), -.15);
  assert.equal(a.oneYearRate, 1); assert.equal(b.oneYearRate, 0);
});
test('long pending episode is in matured denominator but is not called a failure', () => {
  const s = stats(series([100,84,70], ['2019-12-31','2020-01-01','2021-01-01']), -.15);
  assert.equal(s.oneYearCount, 1); assert.equal(s.oneYearRate, 0);
  assert.equal(s.pendingCount, 1); assert.equal(s.medianRecoveryDays, null);
});
test('no signals, empty input, invalid thresholds have no fake percentages', () => {
  for (const s of [stats([], -.15), stats(series([100,99,110]), -.15), stats(series([100,50]), NaN)]) {
    assert.equal(s.signalCount, 0); assert.equal(s.oneYearRate, null);
    assert.equal(s.medianRecoveryDays, null); assert.equal(s.worstAdditionalDd, null);
  }
});
test('calendar days include weekends while condition days count rows', () => {
  const s = stats(series([100,84,100], ['2024-01-04','2024-01-05','2024-01-08']), -.15);
  assert.equal(s.daysBelow, 1); assert.equal(s.episodes[0].recoveryDays, 3);
  assert.equal(calendarDaysBetween('2024-02-28', '2024-03-01'), 2);
});
test('median uses only completed episodes; ongoing low remains in worst drop', () => {
  const s = stats(series([100,84,100,84,84,84,100,84,50]), -.15);
  assert.equal(s.medianRecoveryDays, 2); assert.equal(s.pendingCount, 1);
  assert.ok(Math.abs(s.worstAdditionalDd - (50 / 84 - 1)) < 1e-12);
});
test('engine separates USD/KRW, respects end cutoff and handles deeper empty bands', () => {
  const rows = [100,84,100,80].map((v,i) => ({date:`2020-01-0${i+1}`,closeUsd:v,closeKrw:100}));
  const settings = { targetDdPct: -15 };
  const usd = runEngine(rows,'closeUsd','2020-01-01','2020-01-03',settings);
  const krw = runEngine(rows,'closeKrw','2020-01-01','2020-01-03',settings);
  assert.equal(usd.target.signalCount,1); assert.equal(usd.target.pendingCount,0);
  assert.equal(krw.target.signalCount,0);
  assert.equal(usd.fixedDdTable.find(r => r.ddPct === -50).oneYearRate, null);
  assert.equal(usd.target.episodes.length, 1);
});
