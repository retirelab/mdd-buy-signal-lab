const {test} = require('node:test');
const assert = require('node:assert/strict');
const Q = require('../quote-tools.js');
const now = Date.parse('2026-11-02T15:00:00Z');
function payload(extra={}) {return {version:1,readAt:new Date(now).toISOString(),quotes:{SPY:{symbol:'SPY',currency:'USD',price:97,previousClose:100,tradeAt:'2026-11-02T14:45:00Z',delayMinutes:15,...extra}},fx:{rate:1400,readAt:new Date(now).toISOString()}};}
const engine = {timeSeries:[{date:'2026-10-30',price:100}],metrics:{ath:110},target:{price:88}};
test('only genuine Apps Script exec URLs without embedded query/credentials are accepted',()=>{
 assert.ok(Q.endpointUrl('https://script.google.com/macros/s/ABCDEFGHIJK123/exec'));
 for(const u of ['https://evil.example/macros/s/ABCDEFGHIJK123/exec','javascript:alert(1)','https://script.google.com/macros/s/ABCDEFGHIJK123/dev','https://user:secret@script.google.com/macros/s/ABCDEFGHIJK123/exec','https://script.google.com/macros/s/ABCDEFGHIJK123/exec?token=secret']) assert.throws(()=>Q.endpointUrl(u));
});
test('quote normalization preserves trade time separately from retrieval time',()=>{
 const q=Q.normalize(payload(),'SPY',now);
 assert.equal(q.price,97);assert.equal(q.fx,1400);assert.equal(q.ageMinutes,15);
 assert.notEqual(q.tradeAt,q.fetchedAt);
});
test('wrong symbols, currency, missing timestamps and invalid numeric data cannot become live prices',()=>{
 for(const x of [{price:0},{price:'97'},{price:NaN},{tradeAt:null},{tradeAt:'bad'},{currency:'KRW'},{symbol:'QQQ'},{tradeAt:'2026-11-03T14:45:00Z'},{tradeAt:'2026-10-01T14:45:00Z'}]) assert.throws(()=>Q.normalize(payload(x),'SPY',now));
 assert.throws(()=>Q.normalize(payload(),'QQQ',now));
});
test('stale currency readings are not used for KRW conversion',()=>{
 const p=payload();p.fx.readAt='2026-10-01T00:00:00Z';assert.equal(Q.normalize(p,'SPY',now).fx,null);
});
test('intraday comparison does not mutate recorded metrics and computes extra drop from quote',()=>{
 const before=JSON.stringify(engine);const q=Q.normalize(payload(),'SPY',now);
 const c=Q.compareWithClose(q,engine,'USD',now);
 assert.ok(Math.abs(c.dd-(97/110-1))<1e-12);
 assert.ok(Math.abs(c.remaining-(88/97-1))<1e-12);
 assert.equal(c.reached,false);assert.equal(JSON.stringify(engine),before);
});
test('unreconciled price basis, old source quote and KRW comparison are withheld',()=>{
 const q=Q.normalize(payload(),'SPY',now);
 assert.equal(Q.compareWithClose({...q,previousClose:70},engine,'USD',now),null);
 assert.equal(Q.compareWithClose(q,engine,'USD',now+2*3600000),null);
 assert.equal(Q.compareWithClose(q,engine,'KRW',now),null);
 const historical={...engine,timeSeries:[{date:'2026-11-03',price:100}]};
 assert.equal(Q.compareWithClose(q,historical,'USD',now),null);
});
test('a quote above the closing high has zero reference drawdown',()=>{
 const q=Q.normalize(payload({price:120}),'SPY',now);
 assert.equal(Q.compareWithClose(q,engine,'USD',now).dd,0);
});

test('JSONP resolves and removes its callback and script, and cancellation cleans up',async()=>{
 const oldDocument=global.document; let element,removed=0;
 global.document={createElement:()=>({remove(){removed++;}}),head:{appendChild(e){element=e;}}};
 try {
  const a=Q.request('https://script.google.com/macros/s/ABCDEFGHIJK123/exec');
  const callback=new URL(element.src).searchParams.get('callback');
  global[callback](payload());
  assert.equal((await a.promise).version,1);
  assert.equal(global[callback],undefined);assert.equal(removed,1);
  const b=Q.request('https://script.google.com/macros/s/ABCDEFGHIJK123/exec');
  const otherCallback=new URL(element.src).searchParams.get('callback');
  b.cancel();await assert.rejects(b.promise,/조회 취소/);
  assert.equal(global[otherCallback],undefined);assert.equal(removed,2);
 } finally {global.document=oldDocument;}
});

test('Apps Script response serializes quote Dates and rejects callback injection',()=>{
 const fs=require('node:fs'),vm=require('node:vm');
 const source=fs.readFileSync(require('node:path').join(__dirname,'..','scripts','google_quotes.gs'),'utf8');
 const c=vm.createContext({});
 vm.runInContext(`
  const ContentService={MimeType:{JSON:'json',JAVASCRIPT:'js'},createTextOutput:text=>({text,setMimeType(type){this.type=type;return this;}})};
  const PropertiesService={getScriptProperties:()=>({getProperty:()=> 'fake-sheet-id'})};
  const SpreadsheetApp={openById:()=>({getSheetByName:()=>({getRange:range=>({getValues:()=>[['SPY','NYSEARCA:SPY',97,new Date('2026-11-02T14:45:00Z'),15,100,'USD']],getValue:()=>1400})})})};
  ${source}
  this.ok=doGet({parameter:{callback:'__retirelabQuote1234567890123'}});
  this.bad=doGet({parameter:{callback:'x);alert(1);//'}});
 `,c);
 assert.ok(c.ok.text.startsWith('__retirelabQuote1234567890123('));
 const p=JSON.parse(c.ok.text.slice(c.ok.text.indexOf('(')+1,-2));
 assert.equal(p.quotes.SPY.tradeAt,'2026-11-02T14:45:00.000Z');
 assert.equal(JSON.parse(c.bad.text).error,'invalid callback');
});

function liveEngine(mode='USD') {
 const p=mode==='USD'?100:120000;
 return {analysisStart:'2020-01-01',analysisEnd:'2026-12-31',metrics:{ath:p*1.1,currentPrice:p,currentDd:100/110-1,mdd:-.4},target:{dd:-.2,price:p*.88,isTriggered:false,signalCount:2,episodes:[{entryDate:'2020-01-01'}]},fixedDdTable:[{signalCount:2}],timeSeries:[{date:'2026-10-30',price:p,ath:p*1.1,dd:100/110-1}]};
}

test('new trading date updates prices, drawdown, target gate and appends only a display point',()=>{
 const base=liveEngine(),before=JSON.stringify(base);
 const q=Q.normalize(payload({price:80}),'SPY',now);
 const out=Q.applySnapshot(base,q,'USD',[],'SPY');
 assert.equal(out.metrics.currentPrice,80);assert.equal(out.target.isTriggered,true);
 assert.equal(out.timeSeries.length,2);assert.equal(out.timeSeries.at(-1).date,'2026-11-02');
 assert.equal(out.timeSeries.at(-1).snapshot,true);
 assert.equal(out.fixedDdTable,base.fixedDdTable);assert.equal(out.target.episodes,base.target.episodes);
 assert.equal(JSON.stringify(base),before);
});

test('last-session quote can update KRW conversion after a weekend without inventing a trading day',()=>{
 const base=liveEngine('KRW'),q=Q.normalize(payload({price:100,tradeAt:'2026-10-30T20:00:00Z'}),'SPY',now);
 const out=Q.applySnapshot(base,q,'KRW',[{date:'2026-10-30',closeUsd:100}],'SPY');
 assert.equal(out.metrics.currentPrice,140000);assert.equal(out.metrics.ath,140000);
 assert.equal(out.metrics.currentDd,0);assert.equal(out.target.price,112000);
 assert.equal(out.timeSeries.length,1);assert.equal(out.timeSeries[0].date,'2026-10-30');
 assert.equal(base.timeSeries[0].price,120000);
});

test('missing FX, mismatched symbols, historic end dates and mismatched quote basis keep closes',()=>{
 const base=liveEngine(),q=Q.normalize(payload(),'SPY',now);
 assert.equal(Q.applySnapshot(base,q,'USD',[],'QQQ'),base);
 const historic={...base,analysisEnd:'2026-10-30'};
 assert.equal(Q.applySnapshot(historic,q,'USD',[],'SPY'),historic);
 assert.equal(Q.applySnapshot(base,{...q,previousClose:70},'USD',[],'SPY'),base);
 const krw=liveEngine('KRW');
 assert.equal(Q.applySnapshot(krw,{...q,fx:null},'KRW',[{date:'2026-10-30',closeUsd:100}],'SPY'),krw);
});

test('chart model receives the very same ephemeral snapshot while historical series stays unchanged',()=>{
 const vm=require('node:vm'),fs=require('node:fs'),path=require('node:path');
 const html=fs.readFileSync(path.join(__dirname,'..','index.html'),'utf8');
 const code=html.slice(html.indexOf('    // CHART_ENGINE_START'),html.indexOf('    // CHART_ENGINE_END'));
 const c=vm.createContext({});vm.runInContext(code+';this.build=buildChartModel;',c);
 const base=liveEngine(),q=Q.normalize(payload({price:80}),'SPY',now);
 const view=Q.applySnapshot(base,q,'USD',[],'SPY');
 const model=c.build(view.timeSeries,null);
 assert.equal(model.rows.at(-1).usdPrice,view.metrics.currentPrice);
 assert.equal(model.rows.at(-1).usdDd,view.metrics.currentDd);
 assert.equal(model.rows.at(-1).usdSnapshot,true);
 assert.equal(base.timeSeries.length,1);
});
