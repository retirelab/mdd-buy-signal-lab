/** RetireLab Google Finance quote bridge. Run setup once, then deploy as a web app.
 * Only public market data is returned. Never add portfolio/account data to this sheet.
 * GOOGLEFINANCE may be delayed. Reading/refreshing this endpoint does not force a source update.
 */
const SYMBOLS = [
  ['SPY', 'NYSEARCA:SPY'], ['QQQ', 'NASDAQ:QQQ'], ['HYG', 'NYSEARCA:HYG'],
  ['GLD', 'NYSEARCA:GLD'], ['TLT', 'NASDAQ:TLT']
];

function setup() {
  const props = PropertiesService.getScriptProperties();
  let ss;
  const existing = props.getProperty('QUOTE_SPREADSHEET_ID');
  if (existing) ss = SpreadsheetApp.openById(existing);
  else {
    ss = SpreadsheetApp.create('RetireLab Google Finance 시세 전용');
    props.setProperty('QUOTE_SPREADSHEET_ID', ss.getId());
  }
  ss.setSpreadsheetTimeZone('America/New_York');
  const sheet = ss.getSheetByName('Quotes') || ss.insertSheet('Quotes');
  sheet.getRange('A1:G1').setValues([['symbol','ticker','price','tradeAt','delayMinutes','previousClose','currency']]);
  const rows = SYMBOLS.map(([symbol,ticker],i) => {
    const n = i + 2;
    return [symbol,ticker,`=GOOGLEFINANCE(B${n},"price")`,`=GOOGLEFINANCE(B${n},"tradetime")`,
      `=GOOGLEFINANCE(B${n},"datadelay")`,`=GOOGLEFINANCE(B${n},"closeyest")`,`=GOOGLEFINANCE(B${n},"currency")`];
  });
  sheet.getRange(2,1,rows.length,7).setValues(rows);
  sheet.getRange('D2:D6').setNumberFormat('yyyy-mm-dd hh:mm:ss');
  sheet.getRange('I1:J2').setValues([['pair','rate'],['USDKRW','=GOOGLEFINANCE("CURRENCY:USDKRW")']]);
  SpreadsheetApp.flush();
  console.log('시세 전용 시트: ' + ss.getUrl());
  return ss.getUrl();
}

function doGet(e) {
  const callback = String(e && e.parameter && e.parameter.callback || '');
  if (callback && !/^__retirelabQuote[0-9]{10,40}$/.test(callback)) {
    return ContentService.createTextOutput('{"error":"invalid callback"}').setMimeType(ContentService.MimeType.JSON);
  }
  let payload;
  try {
    const id = PropertiesService.getScriptProperties().getProperty('QUOTE_SPREADSHEET_ID');
    if (!id) throw new Error('Run setup first');
    const sheet = SpreadsheetApp.openById(id).getSheetByName('Quotes');
    const rows = sheet.getRange('A2:G6').getValues();
    const quotes = {};
    rows.forEach(row => {
      const [symbol,,price,tradeAt,delayMinutes,previousClose,currency] = row;
      if (!SYMBOLS.some(s => s[0] === symbol) || typeof price !== 'number' || !(price > 0) ||
          !(tradeAt instanceof Date) || isNaN(tradeAt.getTime())) return;
      quotes[symbol] = { symbol, price, tradeAt: tradeAt.toISOString(),
        delayMinutes: typeof delayMinutes === 'number' ? delayMinutes : null,
        previousClose: typeof previousClose === 'number' ? previousClose : null,
        currency: String(currency) };
    });
    const rate = sheet.getRange('J2').getValue();
    const readAt = new Date().toISOString();
    payload = { version:1, source:'Google Finance via Google Sheets', readAt, quotes,
      fx: typeof rate === 'number' && rate > 0 ? {rate,readAt} : null };
  } catch (error) { payload = { version:1, error:String(error.message), quotes:{} }; }
  const json = JSON.stringify(payload).replace(/</g,'\\u003c').replace(/\u2028/g,'\\u2028').replace(/\u2029/g,'\\u2029');
  return ContentService.createTextOutput(callback ? callback + '(' + json + ');' : json)
    .setMimeType(callback ? ContentService.MimeType.JAVASCRIPT : ContentService.MimeType.JSON);
}
