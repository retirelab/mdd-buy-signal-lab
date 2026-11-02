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
