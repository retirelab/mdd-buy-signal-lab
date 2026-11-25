# VIX (v2026.11.25.2)

Source: Cboe VIX daily close via FRED VIXCLS (public keyless CSV: https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS).
History 1990-01-02 ~ : rows before the first FRED run were backfilled from the Cboe daily file (github.com/datasets/finance-vix); FRED values win on overlap.

Collector: `scripts/update_vix.py`, workflow `update-vix.yml` (Tue–Sat 14:25 UTC = 23:25 Asia/Seoul, also on collector/workflow changes on main and manual runs). Same safety pattern as UNRATE: history never shrinks, a missing FRED value never erases a stored one, failed downloads only update `data/macro/VIXCLS-status.json`.

UI (보조지표 tab, below 미국 실업률): latest close and change vs previous valid close, position inside the last 1-year low–high range, percentile over the whole stored history, zone label (<15 안정 · 15–20 보통 · 20–30 불안 · ≥30 공포), expandable 1y/5y/10y/all chart with a dashed 30 reference line, last 20 trading days table. Logic lives in `vix-tools.js`.
VIX is shown as supplementary information only; it is not part of the existing buy conditions or recovery statistics.

Related: `data/history/fear_greed_daily.json` (CNN Fear & Greed by US trading date, 2011~) is collected by `update-fear-greed-history.yml` for backtests; the app's buy condition still uses `data/fear_greed.json`.

Tests: `node --test tests/*.test.cjs` and `python -m unittest tests/test_vix.py tests/test_fear_greed_history.py`.
