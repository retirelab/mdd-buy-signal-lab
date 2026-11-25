# Per-asset buy thresholds

Decided on 2026-11-25 (first-tranche thresholds, drawdown from the high since 2010-01-01):

| Asset | 1st | 2nd (reference) | 3rd (reference) |
|---|---|---|---|
| SPY | -20% | -30% | -40% |
| QQQ | -20% | -35% | -50% |
| KODEX 200 | -30% | -40% | -50% |

- `asset-settings.js` holds `DEFAULT_TARGETS` and `PRESET_VERSION`.
- Saved browser settings older than `PRESET_VERSION` are moved to these values once; edits made after that are kept per asset.
- Assets without a standard (HYG, GLD, TLT) fall back to -20%.
- The 2nd and 3rd tranches are reference levels used in the daily report; the app's buy condition uses the 1st threshold.

Basis: backtest of episodes (one per drawdown until the prior high is recovered), 2010~ and full history, with and without the Fear & Greed < 25 condition. For SPY and QQQ at -20% or deeper the Fear & Greed condition did not change any entry date in 2011~. Sample sizes are 2–5 episodes per asset and threshold.
