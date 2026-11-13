# v2026.11.13.1
Dashboard order: per-asset drawdown target and current drawdown, market/price diagnosis, compact quote, price/drawdown charts, historical episodes. Indicators and momentum moved to the Auxiliary Indicators tab, replacing the former Recovery Statistics tab; dashboard episode history is retained. Quote connection editor moved to Settings, retaining the same browser storage key. Dashboard stays mounted across tabs to preserve its quote fetch controller and displayed snapshot.

AssetSettings normalizes legacy thresholds to the previously selected symbol; unseen symbols use -20%. Target edits, presets and chart sliders save only the active symbol. Symbol changes load the stored target. Regression tests cover migration, switching and persistence.
