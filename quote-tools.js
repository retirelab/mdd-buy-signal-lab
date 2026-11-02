(function (root) {
  'use strict';
  function endpointUrl(value) {
    const url = new URL(value);
    if (url.protocol !== 'https:' || url.hostname !== 'script.google.com' ||
        !/^\/macros\/s\/[A-Za-z0-9_-]{10,256}\/exec$/.test(url.pathname) || url.username || url.password || url.search || url.hash) {
      throw new Error('Google Apps Script 웹앱의 /exec 주소를 입력해주세요.');
    }
    return url.href;
  }
  function normalize(payload, symbol, now = Date.now()) {
    const q = payload && payload.quotes && payload.quotes[symbol];
    if (!payload || payload.version !== 1 || !q || q.symbol !== symbol || q.currency !== 'USD' ||
        !Number.isFinite(q.price) || q.price <= 0) throw new Error('선택 종목의 유효한 달러 시세가 없습니다.');
    const time = Date.parse(q.tradeAt);
    if (!Number.isFinite(time) || time > now + 300000 || now - time > 7 * 86400000) {
      throw new Error('거래 시각이 없거나 너무 오래된 시세입니다.');
    }
    const fx = payload.fx;
    return {
      symbol, price: q.price, currency: 'USD', tradeAt: new Date(time).toISOString(),
      previousClose: Number.isFinite(q.previousClose) && q.previousClose > 0 ? q.previousClose : null,
      delayMinutes: Number.isFinite(q.delayMinutes) && q.delayMinutes >= 0 ? q.delayMinutes : null,
      sourceReadAt: Number.isFinite(Date.parse(payload.readAt)) ? payload.readAt : null,
      fetchedAt: new Date(now).toISOString(),
      fx: fx && Number.isFinite(fx.rate) && fx.rate > 0 && Number.isFinite(Date.parse(fx.readAt)) &&
        Math.abs(now - Date.parse(fx.readAt)) < 30 * 60000 ? fx.rate : null,
      ageMinutes: Math.max(0, (now - time) / 60000)
    };
  }
  function compareWithClose(quote, engine, mode, now = Date.now()) {
    if (!quote || !engine || !engine.timeSeries.length) return null;
    const last = engine.timeSeries[engine.timeSeries.length - 1];
    const tradeDate = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date(quote.tradeAt));
    // USD quotes are unadjusted. Withhold comparison when the stored close cannot be reconciled.
    // KRW is a display conversion only: the currency feed has no verifiable trade timestamp.
    if (mode !== 'USD') return null;
    if (tradeDate < last.date || now - Date.parse(quote.tradeAt) > 60 * 60000) return null;
    const anchor = tradeDate === last.date ? quote.price : quote.previousClose;
    if (!(anchor > 0) || Math.abs(anchor / last.price - 1) > .005) return null;
    const referenceHigh = engine.metrics.ath;
    return { dd: Math.min(0, quote.price / referenceHigh - 1),
      remaining: engine.target.price / quote.price - 1,
      reached: quote.price <= engine.target.price,
      targetPrice: engine.target.price };
  }
  let sequence = 0;
  function marketDate(value) {
    return new Intl.DateTimeFormat('en-CA', {timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(value));
  }
  function applySnapshot(engine, quote, mode, rawData, expectedSymbol) {
    if (!engine || !quote || !engine.timeSeries.length || (expectedSymbol && quote.symbol !== expectedSymbol)) return engine;
    const date = marketDate(quote.tradeAt);
    const last = engine.timeSeries[engine.timeSeries.length - 1];
    if (date < last.date || date > (engine.analysisEnd || '9999-12-31') || date < (engine.analysisStart || '0000-01-01')) return engine;
    if (Date.parse(date) - Date.parse(last.date) > 7 * 86400000) return engine;
    const reference = mode === 'USD' ? last.price : (rawData || []).find(r => r.date === last.date)?.closeUsd;
    const anchor = date === last.date ? quote.price : quote.previousClose;
    if (!(reference > 0) || !(anchor > 0) || Math.abs(anchor / reference - 1) > .005) return engine;
    const price = mode === 'USD' ? quote.price : quote.fx ? Math.round(quote.price * quote.fx * 100) / 100 : null;
    if (!(price > 0)) return engine;
    // Ephemeral display copy only. Never pass this series into historical recovery calculations.
    const ath = Math.max(engine.metrics.ath, price);
    const dd = price / ath - 1;
    const point = {date,price,ath,dd,snapshot:true};
    const rows = date === last.date ? [...engine.timeSeries.slice(0,-1),point] : [...engine.timeSeries,point];
    const targetPrice = ath * (1 + engine.target.dd);
    return {...engine, timeSeries:rows,
      quoteInfo:{tradeAt:quote.tradeAt,fetchedAt:quote.fetchedAt,sourceReadAt:quote.sourceReadAt,derivedFx:mode==='KRW'},
      metrics:{...engine.metrics,ath,currentPrice:price,currentDd:dd},
      target:{...engine.target,price:targetPrice,isTriggered:dd<=engine.target.dd,
        gapToTarget:targetPrice>0 ? price/targetPrice-1 : null}};
  }
  function request(endpoint) {
    let script, timer, callback, rejectPromise, settled = false;
    function clean() { clearTimeout(timer); if (script) script.remove(); if (callback) delete root[callback]; }
    function fail(error) { if (settled) return; settled = true; clean(); rejectPromise(error); }
    const promise = new Promise((resolve, reject) => {
      rejectPromise = reject;
      try {
        const url = new URL(endpointUrl(endpoint));
        callback = '__retirelabQuote' + Date.now() + (++sequence);
        root[callback] = payload => { if (!settled) { settled = true; clean(); resolve(payload); } };
        url.searchParams.set('callback', callback);
        url.searchParams.set('_', String(Date.now()));
        script = document.createElement('script'); script.src = url.href; script.async = true;
        script.onerror = () => fail(new Error('시세 연결에 실패했습니다. 웹앱 배포 주소와 공개 접근 설정을 확인해주세요.'));
        timer = setTimeout(() => fail(new Error('시세 조회 시간이 초과됐습니다. 최근 종가를 참고해주세요.')), 25000);
        document.head.appendChild(script);
      } catch (error) { fail(error); }
    });
    return { promise, cancel: () => fail(new Error('조회 취소')) };
  }
  const api = { endpointUrl, normalize, compareWithClose, marketDate, applySnapshot, request };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.QuoteTools = api;
})(typeof window === 'undefined' ? globalThis : window);
