const {test}=require('node:test');
const assert=require('node:assert/strict');
const I=require('../indicator-tools.js'),Q=require('../quote-tools.js');
const prices=values=>values.map((price,i)=>({date:new Date(Date.UTC(2020,0,1+i)).toISOString().slice(0,10),price}));
const seed=[...Array(140).fill(80),...Array(60).fill(100)];
const near=(a,b)=>assert.ok(Math.abs(a-b)<1e-8,`${a} != ${b}`);
test('60 and 200 trading-day averages use all available warmup data',()=>{
 const rows=prices(Array.from({length:250},(_,i)=>100+i));const m=I.movingAverageTrend(rows);
 near(m.sma60,319.5);near(m.sma200,249.5);near(m.gap,319.5/249.5-1);
 assert.equal(m.position,'above');assert.equal(m.lastCross,null);
 assert.equal(m.rows[58].sma60,null);near(m.rows[59].sma60,129.5);
 assert.equal(m.rows[198].sma200,null);assert.equal(m.rows[199].sma200,199.5);
});
test('first valid observation is a position and cannot invent a historic golden cross',()=>{
 const m=I.movingAverageTrend(prices(seed));assert.equal(m.position,'above');assert.equal(m.crosses.length,0);
 const short=I.movingAverageTrend(prices(seed.slice(0,199)));assert.equal(short.position,'insufficient');
 assert.equal(I.movingAverageTrend([]).position,'insufficient');
});
test('death cross is recorded once on the exact crossing day, not repeated every day below',()=>{
 const rows=prices([...seed,...Array(40).fill(20)]);const m=I.movingAverageTrend(rows);
 assert.equal(m.crosses.length,1);assert.equal(m.lastCross.type,'death');
 assert.equal(m.lastCross.date,rows[213].date);assert.equal(m.lastCross.tradingDaysAgo,26);
 assert.equal(m.rows[212].cross,null);assert.equal(m.rows[213].cross,'death');
 assert.equal(m.rows.at(-1).cross,null);
});
test('later recovery creates one new golden cross and replaces last event',()=>{
 const m=I.movingAverageTrend(prices([...seed,...Array(200).fill(20),...Array(180).fill(120)]));
 assert.deepEqual(m.crosses.map(c=>c.type),['death','golden']);assert.equal(m.position,'above');
 const c=m.lastCross;const row=m.rows[c.index],prior=m.rows[c.index-1];
 assert.ok(row.sma60>row.sma200);assert.ok(prior.sma60<=prior.sma200);
});
test('touching equality then returning to the same side creates no false crossing',()=>{
 const touch=[...seed,...Array(13).fill(20),60];
 const equal=I.movingAverageTrend(prices(touch));assert.equal(equal.position,'equal');assert.equal(equal.crosses.length,0);
 const bounce=I.movingAverageTrend(prices([...touch,200]));assert.equal(bounce.position,'above');assert.equal(bounce.crosses.length,0);
 const through=I.movingAverageTrend(prices([...touch,20]));assert.equal(through.lastCross.type,'death');
 assert.equal(through.lastCross.index,touch.length);
});
test('a refreshed same-day price can signal provisional crossing without altering confirmed history',()=>{
 const rows=prices([...seed,...Array(13).fill(20),60.1]);const original=JSON.stringify(rows);
 const raw=rows.map(r=>({date:r.date,closeUsd:r.price}));const last=rows.at(-1);
 const base={timeSeries:rows,metrics:{ath:100},target:{dd:-.2}};
 const q={symbol:'SPY',price:59.9,tradeAt:last.date+'T20:00:00Z'};
 const view=Q.applySnapshot(base,q,'USD',raw,'SPY');
 assert.equal(view.timeSeries.length,rows.length);
 const confirmed=I.movingAverageTrend(rows),provisional=I.compute(view.timeSeries,[]).momentum;
 assert.equal(confirmed.position,'above');assert.equal(confirmed.lastCross,null);
 assert.equal(provisional.position,'below');assert.equal(provisional.lastCross.type,'death');assert.equal(provisional.lastCross.tradingDaysAgo,0);
 assert.equal(JSON.stringify(rows),original);
});
test('calendar gaps count observed trading sessions, duplicate dates cannot add extra weights',()=>{
 const rows=prices([...seed,...Array(20).fill(20)]);rows[214].date='2023-01-01';
 // Reordering and duplicate observations are normalized consistently.
 const duplicated=[...rows,rows[0]].reverse();const before=JSON.stringify(duplicated);
 const m=I.movingAverageTrend(duplicated);assert.equal(m.rows.length,rows.length);
 assert.equal(m.lastCross.tradingDaysAgo,m.rows.length-1-m.lastCross.index);
 assert.equal(JSON.stringify(duplicated),before);
});

test('chart range follows 1/3/5/10 year and maximum windows instead of a fixed 252 rows',()=>{
 const values=Array.from({length:5000},(_,i)=>100+i*.01+Math.sin(i/20));
 const trend=I.movingAverageTrend(prices(values));
 const last=trend.rows.at(-1).date;
 const selected=[1,3,5,10].map(years=>I.momentumRowsInRange(trend,I.yearsBefore(last,years),last));
 for(let i=0;i<selected.length;i++){
  const start=I.yearsBefore(last,[1,3,5,10][i]);
  assert.ok(selected[i][0].date>=start);
  assert.equal(selected[i].at(-1).date,last);
  if(i) assert.ok(selected[i].length>selected[i-1].length);
 }
 const all=I.momentumRowsInRange(trend,'1900-01-01','2099-12-31');
 assert.equal(all.length,5000);assert.equal(all[0].date,trend.rows[0].date);
});
test('cropping the chart preserves averages and crossovers calculated with earlier data',()=>{
 const trend=I.movingAverageTrend(prices([...seed,...Array(40).fill(20)]));
 const before=JSON.stringify(trend);
 const visible=I.momentumRowsInRange(trend,trend.rows[210].date,trend.rows[220].date);
 assert.equal(visible.length,11);near(visible[0].sma60,trend.rows[210].sma60);
 near(visible[0].sma200,trend.rows[210].sma200);
 assert.equal(visible.find(r=>r.cross)?.date,trend.rows[213].date);
 assert.equal(JSON.stringify(trend),before);
});
test('range cutoff handles one day, out-of-data dates and custom end dates',()=>{
 const trend=I.movingAverageTrend(prices(seed));const date=trend.rows.at(-1).date;
 assert.equal(I.momentumRowsInRange(trend,date,date).length,1);
 assert.equal(I.momentumRowsInRange(trend,'2030-01-01','2031-01-01').length,0);
 assert.equal(I.momentumRowsInRange(trend,'2030-01-01','2020-01-01').length,0);
 assert.equal(I.momentumRowsInRange(trend,null,trend.rows[99].date).length,100);
 assert.deepEqual(I.momentumRowsInRange(null),[]);
});
