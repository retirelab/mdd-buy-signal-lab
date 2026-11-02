// Run all regressions with: node --test tests/*.test.cjs
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
const source = html.slice(html.indexOf('    // CHART_ENGINE_START'), html.indexOf('    // CHART_ENGINE_END'));
const context = vm.createContext({});
vm.runInContext(source + ';this.api = { nearestChartIndex, chartX, chartIndexAtPointer, sampleChartExtrema, buildChartModel, chartValueTicks };', context);
vm.runInContext('this.api.buildEpisodeView = buildEpisodeView;', context);
const { nearestChartIndex, chartX, chartIndexAtPointer, sampleChartExtrema, buildChartModel, chartValueTicks, buildEpisodeView } = context.api;

test('episode zoom preserves original prices and index baseline, with context and exact markers', () => {
  const data=Array.from({length:150},(_,i)=>({date:new Date(Date.UTC(2020,0,i+1)).toISOString().slice(0,10),price:100+i,dd:0}));
  const model=buildChartModel(data,null);
  const episode={entryDate:data[40].date,lowDate:data[45].date,breakevenDate:data[50].date,recoveryDate:data[70].date};
  const view=buildEpisodeView(model,episode);
  assert.equal(view.model.rows[0].date,data[25].date);
  assert.equal(view.model.rows.at(-1).date,data[85].date);
  assert.equal(view.model.byDate.get(data[40].date).usdIndex,140);
  assert.equal(view.markers.length,4);
  assert.equal(model.rows.length,150);
  for(const m of view.markers) assert.ok(view.model.sampled.some(r=>r.date===m.date));
});

test('pending cases extend to last data and do not invent recovery markers', () => {
  const model=buildChartModel([{date:'2020-01-01',price:100,dd:0},{date:'2020-01-02',price:80,dd:-.2},{date:'2020-01-03',price:70,dd:-.3}],null);
  const view=buildEpisodeView(model,{entryDate:'2020-01-02',lowDate:'2020-01-03',recoveryDate:null,breakevenDate:null});
  assert.equal(view.markers.length,2);
  assert.equal(view.model.rows.at(-1).date,'2020-01-03');
  assert.equal(buildEpisodeView(model,null).model,model);
});
const timestamp = d => Date.parse(d + 'T00:00:00Z');
const row = (date, price, dd = 0) => ({ date, price, dd });

test('full model retains every trading day while sampled model keeps all four extrema', () => {
  const usd = [], krw = [];
  for (let i = 0; i < 2000; i++) {
    const date = new Date(Date.UTC(2020,0,i+1)).toISOString().slice(0,10);
    usd.push(row(date, 100 + i / 100, -.01));
    krw.push(row(date, 100000 + i, -.02));
  }
  usd[7].price = 900; usd[8].dd = -.8;
  krw[11].price = 900000; krw[12].dd = -.9;
  const model = buildChartModel(usd, krw);
  assert.equal(model.rows.length, 2000);
  for (const i of [0,7,8,11,12,1999]) assert.ok(model.sampled.some(r => r.date === usd[i].date));
  assert.ok(model.sampled.length < model.rows.length / 2);
});

test('inspection selects an original day omitted by drawing reduction', () => {
  const data = Array.from({length:2000}, (_,i) => row(new Date(Date.UTC(2020,0,i+1)).toISOString().slice(0,10), 100+i, -.1));
  const model = buildChartModel(data, null);
  const omitted = model.rows.find(r => !model.sampled.includes(r));
  assert.ok(omitted);
  const i = nearestChartIndex(model.rows, omitted.timestamp);
  assert.equal(model.rows[i].date, omitted.date);
  assert.equal(model.rows[i].usdPrice, model.byDate.get(omitted.date).usdPrice);
});

test('pointer inversion uses the same date geometry including left/right label margins', () => {
  const dates = ['2020-01-01','2020-01-02','2020-01-20','2020-02-01'];
  const model = buildChartModel(dates.map((d,i) => row(d,100+i)), null);
  const rect = {left:40,width:350};
  model.rows.forEach((r,i) => {
    const clientX = rect.left + chartX(r.timestamp,model.rows[0].timestamp,model.rows.at(-1).timestamp) / 1000 * rect.width;
    assert.equal(chartIndexAtPointer(model.rows, clientX, rect), i);
  });
  assert.equal(chartIndexAtPointer(model.rows, -100, rect), 0);
  assert.equal(chartIndexAtPointer(model.rows, 1000, rect), model.rows.length-1);
});

test('injected extrema never shift other dates; axis measures calendar time', () => {
  const first=timestamp('2020-01-01'), last=timestamp('2020-01-31');
  const a=chartX(timestamp('2020-01-02'),first,last);
  const b=chartX(timestamp('2020-01-16'),first,last);
  assert.ok(Math.abs(b - 480) < 1e-9);
  assert.ok(b-a > 10*(a-chartX(first,first,last)));
});

test('weekend snaps to nearest observed day, and equal distance chooses earlier day', () => {
  const rows=buildChartModel([row('2024-01-05',100),row('2024-01-08',101)],null).rows;
  assert.equal(nearestChartIndex(rows,timestamp('2024-01-06')),0);
  assert.equal(nearestChartIndex(rows,timestamp('2024-01-07')),1);
  assert.equal(nearestChartIndex(rows,(rows[0].timestamp+rows[1].timestamp)/2),0);
});

test('single date, empty model and invalid input remain finite and safe', () => {
  assert.equal(buildChartModel(null,null).rows.length,0);
  assert.equal(nearestChartIndex([],timestamp('2020-01-01')),null);
  assert.equal(nearestChartIndex([{timestamp:1}],NaN),null);
  assert.equal(chartX(1,1,1),480);
  assert.equal(chartIndexAtPointer([{timestamp:1}],40,{left:0,width:0}),null);
  assert.equal(nearestChartIndex([{timestamp:1}],-100),0);
});

test('missing currency observations stay null and gap boundaries survive reduction', () => {
  const usd=Array.from({length:2000},(_,i)=>row(new Date(Date.UTC(2020,0,i+1)).toISOString().slice(0,10),100+i));
  const krw=usd.filter((_,i)=>i!==13&&i>=5).map(r=>row(r.date,r.price*1200));
  const m=buildChartModel(usd,krw);
  assert.equal(m.rows[0].krwIndex,null);
  assert.equal(m.rows[5].krwIndex,100);
  for(const i of [4,5,12,13,14]) assert.ok(m.sampled.some(r=>r.date===usd[i].date));
  assert.equal(m.byDate.get(usd[13].date).krwPrice,null);
});

test('drawdown ticks are readable percentages and price ticks stay inside scale', () => {
  const dd=chartValueTicks(-.382,0,true);
  assert.deepEqual(Array.from(dd),[0,-.1,-.2,-.3]);
  const price=chartValueTicks(91,813,false);
  assert.ok(price.length>=3&&price.length<=5);
  assert.ok(price.every(v=>v>=91&&v<=813));
  assert.equal(chartValueTicks(100,100,false).length,0);
});

test('SPY regression retains actual high and 2020 crash low in the shared drawing data', () => {
  const prices=JSON.parse(fs.readFileSync(path.join(__dirname,'..','data','prices','SPY.json'),'utf8')).filter(r=>r.date>='2010-01-01');
  let ath=0;
  const usd=prices.map(p=>{ath=Math.max(ath,p.close);return row(p.date,p.close,p.close/ath-1);});
  const high=usd.reduce((a,b)=>a.price>b.price?a:b);
  const low=usd.reduce((a,b)=>a.dd<b.dd?a:b);
  const m=buildChartModel(usd,null);
  assert.ok(m.sampled.some(r=>r.date===high.date));
  assert.ok(m.sampled.some(r=>r.date===low.date));
  assert.ok(m.sampled.some(r=>r.date==='2020-03-23'));
});
