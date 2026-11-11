const {test}=require('node:test'),assert=require('node:assert/strict');
const T=require('../chart-terminal.js');
test('series preserve real zero drawdown, gaps and chronological uniqueness',()=>{
 const rows=[{date:'2026-11-03',dd:null},{date:'2026-11-02',dd:-.2},{date:'2026-11-02',dd:0},{date:'2026-11-04',dd:NaN},{date:'2026-02-31',dd:-.4}];
 const before=JSON.stringify(rows),s=T.prepareSeries(rows,[{id:'USD',key:'dd',color:'blue'}])[0];
 assert.deepEqual(s.data,[{time:'2026-11-02',value:0},{time:'2026-11-03'},{time:'2026-11-04'}]);
 assert.equal(JSON.stringify(rows),before);
});
test('MA warmup and series colors are retained without synthesizing OHLC or volume',()=>{
 const s=T.prepareSeries([{date:'2026-11-02',price:100,sma60:null},{date:'2026-11-03',price:99,sma60:98}], [{id:'price',key:'price',color:'#2563eb'},{id:'MA60',key:'sma60',color:'#ef4444'}]);
 assert.equal(s[1].color,'#ef4444');assert.deepEqual(s[1].data,[{time:'2026-11-02'},{time:'2026-11-03',value:98}]);
 assert.deepEqual(s[0].data[0],{time:'2026-11-02',value:100});
});
test('episode and crossover markers use only the correct series and observed prices',()=>{
 const series={id:'MA60',color:'red',data:[{time:'2026-11-02'},{time:'2026-11-03',value:100},{time:'2026-11-04',value:101}]};
 const markers=T.markersForSeries(series,[{seriesId:'MA60',date:'2026-11-04',text:'골든'},{seriesId:'MA60',date:'2026-11-02'},{seriesId:'USD',date:'2026-11-03'},{seriesId:'MA60',date:'2020-01-01'}]);
 assert.equal(markers.length,1);assert.equal(markers[0].time,'2026-11-04');assert.equal(markers[0].text,'골든');
});
test('business-day times and percentages are normalized consistently',()=>{
 assert.equal(T.isoDay({year:2026,month:1,day:2}),'2026-01-02');assert.equal(T.isoDay(Date.parse('2026-11-02')/1000),'2026-11-02');
 for(const value of [null,NaN,Infinity,'2026-02-31','bad'])assert.equal(T.isoDay(value),null);
 assert.equal(T.formatValue(-.2,'percent'),'-20.00%');assert.equal(T.formatValue(0,'percent'),'0.00%');assert.equal(T.formatValue(100.5,'usd'),'$100.50');assert.equal(T.formatValue(null,'usd'),'—');
});
test('deep drawdown targets remain visible even outside the recorded price range',()=>{
 const input={priceRange:{minValue:-.2,maxValue:-.01},margins:{above:0,below:0}};
 const result=T.includeReferenceRange(input,[{value:-.5},{value:0},{value:NaN}]);
 assert.deepEqual(result.priceRange,{minValue:-.5,maxValue:0});assert.equal(input.priceRange.minValue,-.2);
 assert.equal(T.includeReferenceRange(null,[{value:-.5}]),null);
});
