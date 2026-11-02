const {test}=require('node:test');
const assert=require('node:assert/strict');
const I=require('../indicator-tools.js');
const Q=require('../quote-tools.js');
const date=i=>new Date(Date.UTC(2020,0,1+i)).toISOString().slice(0,10);
const rows=(n,f=i=>100+i)=>Array.from({length:n},(_,i)=>({date:date(i),price:f(i)}));
const near=(a,b)=>assert.ok(Math.abs(a-b)<1e-8,`${a} != ${b}`);
test('Wilder RSI matches the published worksheet seed and smoothing example',()=>{
 const p=[44.34,44.09,44.15,43.61,44.33,44.83,45.10,45.42,45.84,46.08,45.89,46.03,45.61,46.28,46.28,46,46.03,46.41,46.22,45.64,46.21];
 near(I.rsi(p.slice(0,15)),70.46413502109705);
 near(I.rsi(p),62.880718309962404);
 assert.equal(I.rsi(Array(15).fill(100)),50);
 assert.equal(I.rsi(Array.from({length:15},(_,i)=>100+i)),100);
 assert.equal(I.rsi(Array.from({length:15},(_,i)=>100-i)),0);
 assert.equal(I.rsi(Array(14).fill(100)),null);
});
test('200 day average uses the last 200 observations, not the chart range',()=>{
 const m=I.compute(rows(250),[]);near(m.sma,249.5);near(m.distance,349/249.5-1);
 assert.equal(I.compute(rows(199),[]).sma,null);
});
test('20 return volatility uses sample variance and 252 day annualization',()=>{
 const r=Array.from({length:20},(_,i)=>i%2?.01:-.01);
 near(I.volatility(r),Math.sqrt(20*.0001/19*252));
 assert.equal(I.compute(rows(20),[]).volatility,null);
 const m=I.compute(rows(21,()=>100),[]);assert.equal(m.volatility,0);assert.equal(m.volPercentile,50);
});
test('five year reference excludes old windows and marks insufficient coverage',()=>{
 const m=I.compute(rows(2300,i=>100+Math.sin(i)*4+i/100),[]);
 assert.equal(m.fullFiveYears,true);assert.ok(m.volCount>=1825&&m.volCount<=1827);
 assert.equal(I.compute(rows(100),[]).fullFiveYears,false);
 near(I.percentile(2,[1,2,2,3]),50);
});
test('FX uses actual FX dates, filters future data and replaces same day with queried rate',()=>{
 const p=[{date:'2026-11-02',price:100}];
 const fx=[{date:'2025-10-30',rate:1100},{date:'2025-11-03',rate:1200},{date:'2026-10-30',rate:1300},{date:'2026-11-02',rate:1250},{date:'2026-11-04',rate:2000}];
 const original=JSON.stringify(fx);
 const m=I.compute(p,fx,{date:'2026-11-02',rate:1320,readAt:'2026-11-02T12:00:00Z'});
 assert.equal(m.fx.high,1320);assert.equal(m.fx.low,1200);assert.equal(m.fx.position,100);assert.equal(m.fx.fullYear,true);
 assert.equal(JSON.stringify(fx),original);
 const stored=I.compute([{date:'2026-11-01',price:100}],fx).fx;
 assert.equal(stored.date,'2026-10-30');assert.equal(stored.rate,1300);
 assert.equal(I.compute(p,[]).fx,null);
 assert.equal(I.compute(p,[{date:'2026-11-02',rate:1300}]).fx.position,50);
});
test('same day quote updates all price indicators without adding an extra return or mutating closes',()=>{
 const s=rows(300);const last=s.at(-1);const raw=s.map(r=>({date:r.date,closeUsd:r.price}));
 const base={timeSeries:s,analysisStart:s[0].date,analysisEnd:'2099-01-01',metrics:{ath:last.price},target:{dd:-.35}};
 const quote={symbol:'QQQ',price:last.price-1.9,tradeAt:last.date+'T20:00:00Z'};
 const before=JSON.stringify(base);const updated=Q.applySnapshot(base,quote,'USD',raw,'QQQ');
 assert.equal(updated.timeSeries.length,300);
 const m=I.compute(updated.timeSeries,[]),old=I.compute(s,[]);
 assert.ok(m.rsi<old.rsi);assert.ok(m.sma<old.sma);assert.notEqual(m.volatility,old.volatility);
 assert.equal(JSON.stringify(base),before);
});
test('calendar lookbacks handle leap day',()=>assert.equal(I.yearsBefore('2024-02-29',1),'2023-02-28'));
