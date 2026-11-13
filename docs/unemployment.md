# US unemployment (v2026.11.13.2)

Source: BLS / FRED UNRATE, monthly seasonally adjusted percent.
Public keyless CSV: https://fred.stlouisfed.org/graph/fredgraph.csv?id=UNRATE

The public repository and app require no FRED API key or secrets. Dedicated GitHub Actions workflow checks daily at 14:15 UTC (23:15 Asia/Seoul); scheduler execution may be delayed. It also runs on collector/workflow changes and supports manual runs. Concurrency group matches the price updater to avoid concurrent data commits. GitHub's built-in repository token commits results with the existing contents-write permission pattern.

`data/macro/UNRATE.json` retains all monthly rows, including explicit missing values. Initial user CSV was checked against a live FRED CSV download. Full-history revisions replace values by month; truncated, invalid, duplicated and malformed input is rejected. Failed downloads only update status; the last valid series remains intact. Atomic writes protect local files. Collection status includes checkedAt, ok and latestMonth. Data updatedAt changes only when contents change, distinct from observation month and collection check time; it is not the BLS release date.

The indicator compares the latest month against a simple arithmetic mean of that month plus eleven previous consecutive calendar months. All twelve values must be present; missing months are never silently dropped or replaced with zero. Difference uses percentage points, with an equal display within rounding precision. Latest macro data is independent of the selected asset and price-chart period. Current data must not be used as historical point-in-time trading signals; vintage/release data is needed for that. Existing MDD gates and history are unchanged.

UI: summary, expandable 5-year/10-year/all chart, last 24 months table. Actual history starts in 1948. The October 2025 missing observation remains null, causing affected averages to show insufficient data. The latest October 2026 twelve-month window is complete.

Tests: `node --test tests/*.test.cjs` and `python -m unittest discover -s tests -p 'test_unemployment.py'`.
