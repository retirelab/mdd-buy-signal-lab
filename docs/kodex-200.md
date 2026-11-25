# KODEX 200 support — v2026.11.25.1

KODEX 200 (069500) is third after SPY and QQQ in all asset selectors. The native currency is KRW, with separate drawdown thresholds and no manufactured USD price. US currency preferences remain stored and return when switching back.

Source: NAVER Finance chart endpoint, requested from listing date 2002-10-14. Initial capture has 5,951 completed sessions through 2026-11-24. Yahoo's tested endpoint began in 2007, so the longer provider series was selected. These are provider chart prices; historical prices may be adjusted by the provider. They are not guaranteed original unadjusted trade prices and are not a dividend-reinvestment return series. Do not mix sources/bases within this series. Full refresh allows historical revisions; malformed/truncated/duplicate histories are rejected and previous data is retained.

Keyless collector runs weekdays at 18:15 Asia/Seoul (09:15 UTC), on collector/workflow pushes and manually. Today's bar is excluded before 18:00 Korean time to prevent an intraday value being saved as close. Exchange holidays do not create synthetic rows. Workflow concurrency shares the existing update-price-data group. Collection status lives in 069500-meta.json. No API keys or secrets are needed.

Domestic MDD, historical episodes and technical indicators use the native KRW series. Momentum values/labels use won. CNN Fear & Greed and US unemployment remain US reference indicators; domestic trigger uses the KRW drawdown condition alone. US gates are unchanged. Domestic live Google quote requests are suppressed because the deployed bridge covers US tickers/USD normalization; the card explicitly shows latest recorded close and unsupported intraday status.

Verification: Node regression tests, Python collector tests (timezone/session cutoff, invalid source retention and truncation/duplicate rejection), browser selection ordering, maximum range from listing date, KRW chart/indicator values and return to SPY's USD/KRW views.
