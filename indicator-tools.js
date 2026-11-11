(function (root) {
  'use strict';
  const valid = n => Number.isFinite(n) && n > 0;
  function rowsOf(rows, key, end = '9999-12-31') {
    const dates = new Map();
    for (const row of rows || []) {
      if (/^\d{4}-\d{2}-\d{2}$/.test(row.date) && row.date <= end && valid(row[key])) dates.set(row.date, {date:row.date, value:row[key]});
    }
    return [...dates.values()].sort((a,b) => a.date.localeCompare(b.date));
  }
  function yearsBefore(date, years) {
    const d = new Date(date + 'T00:00:00Z');
    const month = d.getUTCMonth();
    d.setUTCFullYear(d.getUTCFullYear() - years);
    if (d.getUTCMonth() !== month) d.setUTCDate(0);
    return d.toISOString().slice(0,10);
  }
  function rsi(prices, period = 14) {
    if (prices.length <= period) return null;
    let gain = 0, loss = 0;
    for (let i=1;i<prices.length;i++) {
      const change = prices[i] - prices[i-1];
      if (i <= period) { gain += Math.max(change,0)/period; loss += Math.max(-change,0)/period; }
      else { gain = (gain*(period-1)+Math.max(change,0))/period; loss = (loss*(period-1)+Math.max(-change,0))/period; }
    }
    return gain === 0 && loss === 0 ? 50 : loss === 0 ? 100 : 100 - 100/(1+gain/loss);
  }
  function volatility(returns) {
    const mean = returns.reduce((a,b)=>a+b,0)/returns.length;
    return Math.sqrt(returns.reduce((s,r)=>s+(r-mean)**2,0)/(returns.length-1))*Math.sqrt(252);
  }
  // Midrank keeps ties neutral: a completely flat comparison set ranks at 50%.
  function percentile(value, values) {
    const eps = 1e-12;
    return values.length ? 100*(values.filter(v=>v<value-eps).length + values.filter(v=>Math.abs(v-value)<=eps).length/2)/values.length : null;
  }
  function movingAverageTrend(priceRows) {
    const prices = rowsOf(priceRows,'price');
    let sum60 = 0, sum200 = 0, previousSide = 0;
    const crosses = [];
    const rows = prices.map((row,i) => {
      sum60 += row.value;
      sum200 += row.value;
      if (i >= 60) sum60 -= prices[i-60].value;
      if (i >= 200) sum200 -= prices[i-200].value;
      const sma60 = i >= 59 ? sum60/60 : null;
      const sma200 = i >= 199 ? sum200/200 : null;
      const gap = sma200 === null ? null : sma60/sma200-1;
      const side = gap === null || Math.abs(gap) <= 1e-12 ? 0 : gap > 0 ? 1 : -1;
      let cross = null;
      // A first observation is a position, not a crossover. Equality retains
      // the last strict side so touching the line cannot create repeat events.
      if (side && previousSide && side !== previousSide) {
        cross = side > 0 ? 'golden' : 'death';
        crosses.push({type:cross,date:row.date,index:i,sma60,sma200});
      }
      if (side) previousSide = side;
      return {date:row.date,price:row.value,sma60,sma200,gap,cross};
    });
    const last = rows.at(-1);
    const lastCross = crosses.at(-1);
    return {rows,crosses,date:last?.date ?? null,sma60:last?.sma60 ?? null,sma200:last?.sma200 ?? null,
      gap:last?.gap ?? null,position:last?.gap === null || !last ? 'insufficient' : Math.abs(last.gap)<=1e-12 ? 'equal' : last.gap>0 ? 'above' : 'below',
      lastCross:lastCross ? {...lastCross,tradingDaysAgo:rows.length-1-lastCross.index} : null};
  }
  function compute(priceRows, fxRows, liveFx = null) {
    const prices = rowsOf(priceRows,'price');
    const last = prices.at(-1);
    if (!last) return null;
    const values = prices.map(r=>r.value);
    const sma = values.length >= 200 ? values.slice(-200).reduce((a,b)=>a+b,0)/200 : null;
    const returns = values.slice(1).map((v,i)=>Math.log(v/values[i]));
    const volRows = [];
    const fiveYearsAgo = yearsBefore(last.date,5);
    for (let i=19;i<returns.length;i++) {
      const date = prices[i+1].date;
      // The first window before the cutoff is retained only to assess coverage.
      volRows.push({date,value:volatility(returns.slice(i-19,i+1))});
    }
    const vol = volRows.at(-1)?.value ?? null;
    const comparison = volRows.filter(r=>r.date>=fiveYearsAgo).map(r=>r.value);
    const fx = rowsOf(fxRows,'rate',last.date);
    const fxLive = liveFx && valid(liveFx.rate) && /^\d{4}-\d{2}-\d{2}$/.test(liveFx.date) && liveFx.date>=last.date;
    if (fxLive) {
      while (fx.length && fx.at(-1).date>=liveFx.date) fx.pop();
      fx.push({date:liveFx.date,value:liveFx.rate});
    }
    const currentFx = fx.at(-1);
    let fxResult = null;
    if (currentFx) {
      const cutoff = yearsBefore(currentFx.date,1);
      const window = fx.filter(r=>r.date>=cutoff);
      const low = Math.min(...window.map(r=>r.value)), high = Math.max(...window.map(r=>r.value));
      fxResult = {date:currentFx.date,rate:currentFx.value,low,high,position:high===low?50:100*(currentFx.value-low)/(high-low),count:window.length,fullYear:fx[0].date<=cutoff,live:!!fxLive,readAt:fxLive?liveFx.readAt:null};
    }
    return {date:last.date,count:values.length,rsi:rsi(values),sma,distance:sma===null?null:last.value/sma-1,volatility:vol,volPercentile:vol===null?null:percentile(vol,comparison),volCount:comparison.length,fullFiveYears:!!volRows.length&&volRows[0].date<=fiveYearsAgo,fx:fxResult,momentum:movingAverageTrend(priceRows)};
  }
  const api = {compute,rsi,volatility,percentile,yearsBefore,movingAverageTrend};
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.IndicatorTools = api;
})(typeof window === 'undefined' ? globalThis : window);
